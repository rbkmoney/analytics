package com.rbkmoney.analytics;

import com.rbkmoney.analytics.dao.repository.postgres.PostgresBalanceChangesRepository;
import com.rbkmoney.analytics.service.HgClientService;
import com.rbkmoney.analytics.utils.BuildUtils;
import com.rbkmoney.analytics.utils.EventRangeFactory;
import com.rbkmoney.analytics.utils.FileUtil;
import com.rbkmoney.damsel.domain.*;
import com.rbkmoney.damsel.payment_processing.InvoicingSrv;
import com.rbkmoney.machinegun.eventsink.SinkEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.testcontainers.containers.ClickHouseContainer;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Slf4j
@RunWith(SpringRunner.class)
@SpringBootTest(classes = AnalyticsApplication.class,
        properties = {"kafka.state.cache.size=0"})
@ContextConfiguration(initializers = EventSinkListenerTest.Initializer.class)
public class EventSinkListenerTest extends KafkaAbstractTest {

    public static final long MESSAGE_TIMEOUT = 4_000L;
    public static final String SOURCE_ID = "sourceID";
    public static final String FIRST_ADJUSTMENT = "1";
    public static final String SELECT_SUM = "SELECT shopId, sum(amount) as sum " +
            "from %1s " +
            "group by shopId, currency, status " +
            "having shopId = '";

    @ClassRule
    public static ClickHouseContainer clickHouseContainer = new ClickHouseContainer();

    public static class Initializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {
        @Override
        public void initialize(ConfigurableApplicationContext configurableApplicationContext) {
            log.info("clickhouse.db.url={}", clickHouseContainer.getJdbcUrl());
            TestPropertyValues.of(
                    "clickhouse.db.url=" + clickHouseContainer.getJdbcUrl(),
                    "clickhouse.db.user=" + clickHouseContainer.getUsername(),
                    "clickhouse.db.password=" + clickHouseContainer.getPassword())
                    .applyTo(configurableApplicationContext.getEnvironment());
        }
    }

    @MockBean
    InvoicingSrv.Iface invoicingClient;

    @MockBean
    PostgresBalanceChangesRepository postgresBalanceChangesRepository;

    @Autowired
    private JdbcTemplate clickHouseJdbcTemplate;

    @Autowired
    private EventRangeFactory eventRangeFactory;

    @Before
    public void init() throws SQLException, IOException, TException {
        try (Connection connection = getSystemConn()) {
            String sql = FileUtil.getFile("sql/V1__db_init.sql");
            String[] split = sql.split(";");
            for (String exec : split) {
                connection.createStatement().execute(exec);
            }
        }
    }

    private Connection getSystemConn() throws SQLException {
        ClickHouseProperties properties = new ClickHouseProperties();
        ClickHouseDataSource dataSource = new ClickHouseDataSource(clickHouseContainer.getJdbcUrl(), properties);
        return dataSource.getConnection();
    }

    @Test
    public void testEventSink() throws InterruptedException, IOException, TException {
        List<SinkEvent> sinkEvents = MgEventSinkFlowGenerator.generateSuccessFlow(SOURCE_ID);

        mockPayment(SOURCE_ID);

        sinkEvents.forEach(this::produceMessageToEventSink);

        sinkEvents = MgEventSinkFlowGenerator.generateSuccessNotFullFlow("sourceID_2");
        sinkEvents.forEach(this::produceMessageToEventSink);

        Thread.sleep(MESSAGE_TIMEOUT);

        //check sum for captured payment
        long sum = clickHouseJdbcTemplate.queryForObject(
                String.format(SELECT_SUM, "analytic.events_sink") + MgEventSinkFlowGenerator.SHOP_ID
                        + "' and status = 'captured' and currency = 'RUB'", (resultSet, i) -> resultSet.getLong("sum"));

        assertEquals(1000L, sum);

        //statistic for paymentTool
        List<Map<String, Object>> list = clickHouseJdbcTemplate.queryForList(
                "SELECT shopId, paymentTool," +
                        "( SELECT count() from analytic.events_sink " +
                        "group by shopId, currency " +
                        "having shopId = '" + MgEventSinkFlowGenerator.SHOP_ID + "' and currency = 'RUB') as total_count, " +
                        "count() * 100 / total_count as sum " +
                        "from analytic.events_sink " +
                        "group by shopId, currency, paymentTool " +
                        "having shopId = '" + MgEventSinkFlowGenerator.SHOP_ID + "' and currency = 'RUB'");

        list.forEach(stringObjectMap -> {
                    Object cnt = stringObjectMap.get("sum");
                    assertEquals(100.0, cnt);
                    System.out.println(stringObjectMap);
                }
        );

        String sourceID_refund_1 = "sourceID_refund_1";
        mockPayment(sourceID_refund_1);
        mockRefund(sourceID_refund_1, 8, "1");
        mockRefund(sourceID_refund_1, 10, "2");

        // test refund flow
        sinkEvents = MgEventSinkFlowGenerator.generateRefundedFlow(sourceID_refund_1);
        sinkEvents.forEach(this::produceMessageToEventSink);

        Thread.sleep(MESSAGE_TIMEOUT);

        //check sum for succeeded refund
        sum = clickHouseJdbcTemplate.queryForObject(
                String.format(SELECT_SUM, "analytic.events_sink_refund") + MgEventSinkFlowGenerator.SHOP_ID
                        + "' and status = 'succeeded' and currency = 'RUB'", (resultSet, i) -> resultSet.getLong("sum"));

        assertEquals(246L, sum);

        //check collapsing sum for pending refund
        List<Map<String, Object>> resultList = clickHouseJdbcTemplate.queryForList(
                String.format(SELECT_SUM, "analytic.events_sink_refund") + MgEventSinkFlowGenerator.SHOP_ID
                        + "' and status = 'pending' and currency = 'RUB'");

        assertTrue(resultList.isEmpty());

        String source_adjustment = "source_adjustment";
        mockPayment(source_adjustment);
        mockAdjustment(source_adjustment, 7, FIRST_ADJUSTMENT);

        sinkEvents = MgEventSinkFlowGenerator.generateSuccessAdjustment(source_adjustment);
        sinkEvents.forEach(this::produceMessageToEventSink);

        Thread.sleep(MESSAGE_TIMEOUT);

        //check sum for succeeded refund
        sum = clickHouseJdbcTemplate.queryForObject(
                String.format(SELECT_SUM, "analytic.events_sink_adjustment") + MgEventSinkFlowGenerator.SHOP_ID
                        + "' and status = 'captured' and currency = 'RUB'", (resultSet, i) -> resultSet.getLong("sum"));

        assertEquals(23L, sum);

        List<LocalDate> localDates = clickHouseJdbcTemplate.queryForList(
                "SELECT timestamp from analytic.events_sink ", LocalDate.class);

        System.out.println(localDates);
    }

    private void mockPayment(String sourceId) throws TException, IOException {
        Mockito.when(invoicingClient.get(HgClientService.USER_INFO, sourceId, eventRangeFactory.create(6)))
                .thenReturn(BuildUtils.buildInvoice(MgEventSinkFlowGenerator.PARTY_ID, MgEventSinkFlowGenerator.SHOP_ID,
                        sourceId, "1", "1", FIRST_ADJUSTMENT,
                        InvoiceStatus.paid(new InvoicePaid()), InvoicePaymentStatus.pending(new InvoicePaymentPending())));
    }

    private void mockRefund(String sourceId, int sequenceId, String refundId) throws TException, IOException {
        Mockito.when(invoicingClient.get(HgClientService.USER_INFO, sourceId, eventRangeFactory.create(sequenceId)))
                .thenReturn(BuildUtils.buildInvoice(MgEventSinkFlowGenerator.PARTY_ID, MgEventSinkFlowGenerator.SHOP_ID,
                        sourceId, "1", refundId, FIRST_ADJUSTMENT,
                        InvoiceStatus.paid(new InvoicePaid()), InvoicePaymentStatus.refunded(new InvoicePaymentRefunded())));
    }

    private void mockAdjustment(String sourceId, int sequenceId, String adjustmentId) throws TException, IOException {
        Mockito.when(invoicingClient.get(HgClientService.USER_INFO, sourceId, eventRangeFactory.create(sequenceId)))
                .thenReturn(BuildUtils.buildInvoice(MgEventSinkFlowGenerator.PARTY_ID, MgEventSinkFlowGenerator.SHOP_ID,
                        sourceId, "1", adjustmentId, FIRST_ADJUSTMENT,
                        InvoiceStatus.paid(new InvoicePaid()), InvoicePaymentStatus.captured(new InvoicePaymentCaptured())));
    }

}
