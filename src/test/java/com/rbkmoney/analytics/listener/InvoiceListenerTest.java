package com.rbkmoney.analytics.listener;

import com.rbkmoney.analytics.AnalyticsApplication;
import com.rbkmoney.analytics.dao.repository.postgres.PostgresBalanceChangesRepository;
import com.rbkmoney.analytics.service.HgClientService;
import com.rbkmoney.analytics.utils.BuildUtils;
import com.rbkmoney.analytics.utils.EventRangeFactory;
import com.rbkmoney.analytics.utils.KafkaAbstractTest;
import com.rbkmoney.clickhouse.initializer.ChInitializer;
import com.rbkmoney.damsel.domain.*;
import com.rbkmoney.damsel.geo_ip.GeoIpServiceSrv;
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
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Slf4j
@RunWith(SpringRunner.class)
@SpringBootTest(classes = AnalyticsApplication.class,
        properties = {"kafka.state.cache.size=0"})
@ContextConfiguration(initializers = InvoiceListenerTest.Initializer.class)
public class InvoiceListenerTest extends KafkaAbstractTest {

    private static final long MESSAGE_TIMEOUT = 4_000L;
    private static final String SOURCE_ID = "sourceID";
    private static final String FIRST = "1";
    private static final String SELECT_SUM = "SELECT shopId, sum(amount) as sum " +
            "from %1s " +
            "group by shopId, currency, status " +
            "having shopId = '";

    @ClassRule
    public static ClickHouseContainer clickHouseContainer = new ClickHouseContainer();
    @MockBean
    private GeoIpServiceSrv.Iface iface;
    @MockBean
    private InvoicingSrv.Iface invoicingClient;
    @MockBean
    private PostgresBalanceChangesRepository postgresBalanceChangesRepository;
    @Autowired
    private JdbcTemplate clickHouseJdbcTemplate;
    @Autowired
    private EventRangeFactory eventRangeFactory;

    @Before
    public void init() throws SQLException {
        ChInitializer.initAllScripts(clickHouseContainer, List.of(
                "sql/V1__db_init.sql",
                "sql/V2__add_fields.sql",
                "sql/V3__add_provider_field.sql"
        ));
    }

    private Connection getSystemConn() throws SQLException {
        ClickHouseProperties properties = new ClickHouseProperties();
        ClickHouseDataSource dataSource = new ClickHouseDataSource(clickHouseContainer.getJdbcUrl(), properties);
        return dataSource.getConnection();
    }

    @Test
    public void testEventSink() throws InterruptedException, IOException, TException {
        List<SinkEvent> sinkEvents = InvoiceFlowGenerator.generateSuccessFlow(SOURCE_ID);

        mockPayment(SOURCE_ID);

        sinkEvents.forEach(event -> produceMessageToTopic(this.eventSinkTopic, event));

        sinkEvents = InvoiceFlowGenerator.generateSuccessNotFullFlow("sourceID_2");
        sinkEvents.forEach(event -> produceMessageToTopic(this.eventSinkTopic, event));

        AtomicLong count = new AtomicLong();

        //check sum for captured payment
        await().atMost(60, TimeUnit.SECONDS).until(() -> {
            Thread.sleep(MESSAGE_TIMEOUT);

            try {
                count.set(clickHouseJdbcTemplate.queryForObject(
                        String.format(SELECT_SUM, "analytic.events_sink") + InvoiceFlowGenerator.SHOP_ID +
                                "' and status = 'captured' and currency = 'RUB'",
                        (resultSet, i) -> resultSet.getLong("sum")));
            } catch (Exception e) {
                return false;
            }

            return count.get() == 1000L;
        });

        //statistic for paymentTool
        List<Map<String, Object>> list = clickHouseJdbcTemplate.queryForList(
                "SELECT shopId, paymentTool," +
                        "( SELECT count() from analytic.events_sink " +
                        "group by shopId, currency " +
                        "having shopId = '" + InvoiceFlowGenerator.SHOP_ID +
                        "' and currency = 'RUB') as total_count, " +
                        "count() * 100 / total_count as sum " +
                        "from analytic.events_sink " +
                        "group by shopId, currency, paymentTool " +
                        "having shopId = '" + InvoiceFlowGenerator.SHOP_ID + "' and currency = 'RUB'");

        list.forEach(stringObjectMap -> {
                    Object cnt = stringObjectMap.get("sum");
                    assertEquals(100.0, cnt);
                    System.out.println(stringObjectMap);
                }
        );

        String sourceIDRefundFirst = "sourceID_refund_1";
        mockPayment(sourceIDRefundFirst);
        mockRefund(sourceIDRefundFirst, 8, "1");
        mockRefund(sourceIDRefundFirst, 10, "2");

        // test refund flow
        sinkEvents = InvoiceFlowGenerator.generateRefundedFlow(sourceIDRefundFirst);
        sinkEvents.forEach(event -> produceMessageToTopic(this.eventSinkTopic, event));

        //check sum for succeeded refund
        await().atMost(60, TimeUnit.SECONDS).until(() -> {
            Thread.sleep(MESSAGE_TIMEOUT);
            try {
                count.set(clickHouseJdbcTemplate.queryForObject(
                        String.format(SELECT_SUM, "analytic.events_sink_refund") + InvoiceFlowGenerator.SHOP_ID +
                                "' and status = 'succeeded' and currency = 'RUB'",
                        (resultSet, i) -> resultSet.getLong("sum")));
            } catch (Exception e) {
                return false;
            }

            return count.get() == 246L;
        });

        //check collapsing sum for pending refund
        List<Map<String, Object>> resultList = clickHouseJdbcTemplate.queryForList(
                String.format(SELECT_SUM, "analytic.events_sink_refund") + InvoiceFlowGenerator.SHOP_ID +
                        "' and status = 'pending' and currency = 'RUB'");

        assertTrue(resultList.isEmpty());

        String sourceAdjustment = "source_adjustment";
        mockPayment(sourceAdjustment);
        mockAdjustment(sourceAdjustment, 7, FIRST);

        sinkEvents = InvoiceFlowGenerator.generateSuccessAdjustment(sourceAdjustment);
        sinkEvents.forEach(event -> produceMessageToTopic(this.eventSinkTopic, event));

        //check sum for succeeded refund
        await().atMost(60, TimeUnit.SECONDS).until(() -> {
            Thread.sleep(MESSAGE_TIMEOUT);
            try {
                count.set(clickHouseJdbcTemplate.queryForObject(
                        String.format(SELECT_SUM, "analytic.events_sink_adjustment") +
                                InvoiceFlowGenerator.SHOP_ID + "' and status = 'captured' and currency = 'RUB'",
                        (resultSet, i) -> resultSet.getLong("sum")));
            } catch (Exception e) {
                return false;
            }

            return count.get() == 23L;
        });

        String sourceChargeback = "source_chargeback";
        mockPayment(sourceChargeback);
        mockChargeback(sourceChargeback, 7, FIRST);
        sinkEvents = InvoiceFlowGenerator.generateChargebackFlow(sourceChargeback);
        sinkEvents.forEach(event -> produceMessageToTopic(this.eventSinkTopic, event));


        //check sum for succeeded chargeback
        await().atMost(60, TimeUnit.SECONDS).until(() -> {
            Thread.sleep(20000L);
            try {
                count.set(clickHouseJdbcTemplate.queryForObject(
                        String.format(SELECT_SUM, "analytic.events_sink_chargeback") +
                                InvoiceFlowGenerator.SHOP_ID + "' and status = 'accepted' and currency = 'RUB'",
                        (resultSet, i) -> resultSet.getLong("sum")));
            } catch (Exception e) {
                return false;
            }

            return count.get() == 23L;
        });

    }

    private void mockPayment(String sourceId) throws TException, IOException {
        Mockito.when(invoicingClient.get(HgClientService.USER_INFO, sourceId, eventRangeFactory.create(6)))
                .thenReturn(BuildUtils.buildInvoice(InvoiceFlowGenerator.PARTY_ID, InvoiceFlowGenerator.SHOP_ID,
                        sourceId, "1", "1", FIRST, FIRST,
                        InvoiceStatus.paid(new InvoicePaid()),
                        InvoicePaymentStatus.pending(new InvoicePaymentPending())));
    }

    private void mockRefund(String sourceId, int sequenceId, String refundId) throws TException, IOException {
        Mockito.when(invoicingClient.get(HgClientService.USER_INFO, sourceId, eventRangeFactory.create(sequenceId)))
                .thenReturn(BuildUtils.buildInvoice(InvoiceFlowGenerator.PARTY_ID, InvoiceFlowGenerator.SHOP_ID,
                        sourceId, "1", refundId, "1", "1",
                        InvoiceStatus.paid(new InvoicePaid()),
                        InvoicePaymentStatus.refunded(new InvoicePaymentRefunded())));
    }

    private void mockAdjustment(String sourceId, int sequenceId, String adjustmentId) throws TException, IOException {
        Mockito.when(invoicingClient.get(HgClientService.USER_INFO, sourceId, eventRangeFactory.create(sequenceId)))
                .thenReturn(BuildUtils.buildInvoice(InvoiceFlowGenerator.PARTY_ID, InvoiceFlowGenerator.SHOP_ID,
                        sourceId, "1", "1", "1", adjustmentId,
                        InvoiceStatus.paid(new InvoicePaid()),
                        InvoicePaymentStatus.captured(new InvoicePaymentCaptured())));
    }

    private void mockChargeback(String sourceId, int sequenceId, String chargebackId) throws TException, IOException {
        Mockito.when(invoicingClient.get(HgClientService.USER_INFO, sourceId, eventRangeFactory.create(sequenceId)))
                .thenReturn(BuildUtils.buildInvoice(InvoiceFlowGenerator.PARTY_ID, InvoiceFlowGenerator.SHOP_ID,
                        sourceId, "1", "1", chargebackId, "1",
                        InvoiceStatus.paid(new InvoicePaid()),
                        InvoicePaymentStatus.charged_back(new InvoicePaymentChargedBack())));
    }

    public static class Initializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {
        @Override
        public void initialize(ConfigurableApplicationContext configurableApplicationContext) {
            log.info("clickhouse.db.url={}", clickHouseContainer.getJdbcUrl());
            TestPropertyValues.of(
                    "clickhouse.db.url=" + clickHouseContainer.getJdbcUrl(),
                    "clickhouse.db.user=" + clickHouseContainer.getUsername(),
                    "clickhouse.db.password=" + clickHouseContainer.getPassword(),
                    "spring.flyway.enabled=false")
                    .applyTo(configurableApplicationContext.getEnvironment());
        }
    }
}
