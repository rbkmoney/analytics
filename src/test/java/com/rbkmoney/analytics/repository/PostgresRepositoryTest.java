package com.rbkmoney.analytics.repository;

import com.rbkmoney.analytics.AnalyticsApplication;
import com.rbkmoney.analytics.dao.model.MgAdjustmentRow;
import com.rbkmoney.analytics.dao.model.MgPaymentSinkRow;
import com.rbkmoney.analytics.dao.model.MgRefundRow;
import com.rbkmoney.analytics.dao.repository.postgres.PostgresBalanceChangesRepository;
import com.rbkmoney.analytics.domain.CashFlowResult;
import com.rbkmoney.analytics.listener.MgInvoiceListener;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.testcontainers.containers.PostgreSQLContainer;

import java.sql.Date;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;

import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = RANDOM_PORT)
@ContextConfiguration(classes = AnalyticsApplication.class, initializers = PostgresRepositoryTest.Initializer.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public class PostgresRepositoryTest {

    @ClassRule
    public static PostgreSQLContainer postgres = (PostgreSQLContainer) new PostgreSQLContainer("postgres:9.6")
            .withStartupTimeout(Duration.ofMinutes(5));

    @LocalServerPort
    protected int port;

    @MockBean
    private MgInvoiceListener mgInvoiceListener;

    @Autowired
    private PostgresBalanceChangesRepository postgresBalanceChangesRepository;

    public static class Initializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {
        @Override
        public void initialize(ConfigurableApplicationContext configurableApplicationContext) {
            TestPropertyValues.of(
                    "postgres.db.url=" + postgres.getJdbcUrl(),
                    "postgres.db.user=" + postgres.getUsername(),
                    "postgres.db.password=" + postgres.getPassword(),
                    "spring.flyway.url=" + postgres.getJdbcUrl(),
                    "spring.flyway.user=" + postgres.getUsername(),
                    "spring.flyway.password=" + postgres.getPassword())
                    .and(configurableApplicationContext.getEnvironment().getActiveProfiles())
                    .applyTo(configurableApplicationContext);
        }
    }

    @Test
    public void shouldInsertBatch() {
        postgresBalanceChangesRepository.insertPayments(List.of(payment()));
        postgresBalanceChangesRepository.insertRefunds(List.of(refund()));
        postgresBalanceChangesRepository.insertAdjustments(List.of(adjustment()));
    }

    private MgPaymentSinkRow payment() {
        MgPaymentSinkRow mgPaymentSinkRow = new MgPaymentSinkRow();
        mgPaymentSinkRow.setInvoiceId("invoice_id");
        mgPaymentSinkRow.setSequenceId(1L);
        mgPaymentSinkRow.setTimestamp(Date.valueOf(LocalDate.EPOCH));
        mgPaymentSinkRow.setEventTime(Instant.now().toEpochMilli());
        mgPaymentSinkRow.setCurrency("RUB");
        mgPaymentSinkRow.setPartyId("party_id");
        mgPaymentSinkRow.setShopId("shop_id");
        mgPaymentSinkRow.setCashFlowResult(CashFlowResult.builder()
                .amount(1000L)
                .systemFee(100L)
                .build());

        return mgPaymentSinkRow;
    }

    private MgRefundRow refund() {
        MgRefundRow mgRefundRow = new MgRefundRow();
        mgRefundRow.setInvoiceId("invoice_id");
        mgRefundRow.setSequenceId(2L);
        mgRefundRow.setTimestamp(Date.valueOf(LocalDate.EPOCH));
        mgRefundRow.setEventTime(Instant.now().toEpochMilli());
        mgRefundRow.setCurrency("RUB");
        mgRefundRow.setPartyId("party_id");
        mgRefundRow.setShopId("shop_id");
        mgRefundRow.setCashFlowResult(CashFlowResult.builder()
                .amount(500L)
                .systemFee(50L)
                .build());

        return mgRefundRow;
    }

    private MgAdjustmentRow adjustment() {
        MgAdjustmentRow mgAdjustmentRow = new MgAdjustmentRow();
        mgAdjustmentRow.setInvoiceId("invoice_id");
        mgAdjustmentRow.setSequenceId(3L);
        mgAdjustmentRow.setTimestamp(Date.valueOf(LocalDate.EPOCH));
        mgAdjustmentRow.setEventTime(Instant.now().toEpochMilli());
        mgAdjustmentRow.setCurrency("RUB");
        mgAdjustmentRow.setPartyId("party_id");
        mgAdjustmentRow.setShopId("shop_id");
        mgAdjustmentRow.setCashFlowResult(CashFlowResult.builder()
                .systemFee(250L)
                .build());
        mgAdjustmentRow.setOldCashFlowResult(CashFlowResult.builder()
                .systemFee(100L)
                .build());

        return mgAdjustmentRow;
    }
}
