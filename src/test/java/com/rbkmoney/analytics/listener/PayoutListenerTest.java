package com.rbkmoney.analytics.listener;

import com.rbkmoney.analytics.AnalyticsApplication;
import com.rbkmoney.analytics.dao.repository.postgres.PostgresBalanceChangesRepository;
import com.rbkmoney.analytics.utils.FileUtil;
import com.rbkmoney.analytics.utils.KafkaAbstractTest;
import com.rbkmoney.damsel.domain.CurrencyRef;
import com.rbkmoney.damsel.geo_ip.GeoIpServiceSrv;
import com.rbkmoney.damsel.payout_processing.*;
import com.rbkmoney.geck.common.util.TypeUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
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

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@Slf4j
@RunWith(SpringRunner.class)
@SpringBootTest(classes = AnalyticsApplication.class,
        properties = {"kafka.state.cache.size=0"})
@ContextConfiguration(initializers = PayoutListenerTest.Initializer.class)
public class PayoutListenerTest extends KafkaAbstractTest {

    private static final long MESSAGE_TIMEOUT = 4_000L;
    private static final String SHOP_ID = "SHOP_ID";
    private static final String PAYOUT_ID = "PAYOUT_ID";
    private static final String SELECT_SUM = "SELECT shopId, sum(amount) as sum " +
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
                    "clickhouse.db.password=" + clickHouseContainer.getPassword(),
                    "spring.flyway.enabled=false")
                    .applyTo(configurableApplicationContext.getEnvironment());
        }
    }

    @MockBean
    private GeoIpServiceSrv.Iface iface;

    @MockBean
    private PayoutManagementSrv.Iface payouterClient;

    @MockBean
    private PostgresBalanceChangesRepository postgresBalanceChangesRepository;

    @Autowired
    private JdbcTemplate clickHouseJdbcTemplate;

    @Before
    public void init() throws SQLException {
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
    public void testPayout() throws InterruptedException, TException {
        // Given
        Event payoutEvent = new Event()
                .setId(1L)
                .setPayload(EventPayload.payout_changes(List.of(
                        PayoutChange.payout_status_changed(new PayoutStatusChanged()
                                .setStatus(PayoutStatus.paid(new PayoutPaid()))))))
                .setSource(EventSource.payout_id(PAYOUT_ID))
                .setCreatedAt(TypeUtil.temporalToString(Instant.now()));

        mockPayout();

        // When
        produceMessageToPayout(payoutEvent);
        Thread.sleep(MESSAGE_TIMEOUT);

        // Then
        long sum = clickHouseJdbcTemplate.queryForObject(
                String.format(SELECT_SUM, "analytic.events_sink_payout") + SHOP_ID
                        + "' and status = 'paid' and currency = 'RUB'",
                (resultSet, i) -> resultSet.getLong("sum"));

        assertEquals(10L, sum);
    }

    private void mockPayout() throws TException {
        when(payouterClient.getEvents(eq(PAYOUT_ID), any(EventRange.class)))
                .thenReturn(List.of(
                        new Event()
                                .setId(2L)
                                .setSource(EventSource.payout_id(PAYOUT_ID))
                                .setPayload(EventPayload.payout_changes(List.of(
                                        PayoutChange.payout_created(new PayoutCreated()
                                                .setPayout(new Payout()
                                                        .setShopId(SHOP_ID)
                                                        .setAmount(10L)
                                                        .setCurrency(new CurrencyRef()
                                                                .setSymbolicCode("RUB"))
                                                        .setCreatedAt(TypeUtil.temporalToString(Instant.now()))
                                                        .setType(PayoutType.wallet(new Wallet()))))))),
                        new Event()
                                .setId(3L)
                                .setSource(EventSource.payout_id(PAYOUT_ID))
                                .setPayload(EventPayload.payout_changes(List.of(
                                        PayoutChange.payout_status_changed(new PayoutStatusChanged()
                                                .setStatus(PayoutStatus.paid(new PayoutPaid()))))))));
    }
}
