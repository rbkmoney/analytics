package com.rbkmoney.analytics.repository;

import com.rbkmoney.analytics.AnalyticsApplication;
import com.rbkmoney.analytics.constant.PayoutStatus;
import com.rbkmoney.analytics.dao.model.AdjustmentRow;
import com.rbkmoney.analytics.dao.model.PaymentRow;
import com.rbkmoney.analytics.dao.model.PayoutRow;
import com.rbkmoney.analytics.dao.model.RefundRow;
import com.rbkmoney.analytics.dao.repository.postgres.PostgresBalanceChangesRepository;
import com.rbkmoney.analytics.dao.repository.postgres.party.management.PartyDao;
import com.rbkmoney.analytics.dao.repository.postgres.party.management.ShopDao;
import com.rbkmoney.analytics.domain.CashFlowResult;
import com.rbkmoney.analytics.domain.db.tables.pojos.Party;
import com.rbkmoney.analytics.domain.db.tables.pojos.Shop;
import com.rbkmoney.analytics.utils.Version;
import io.github.benas.randombeans.api.EnhancedRandom;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.testcontainers.containers.PostgreSQLContainer;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = RANDOM_PORT)
@ContextConfiguration(classes = AnalyticsApplication.class, initializers = PostgresRepositoryTest.Initializer.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public class PostgresRepositoryTest {

    @ClassRule
    @SuppressWarnings("rawtypes")
    public static PostgreSQLContainer postgres = (PostgreSQLContainer) new PostgreSQLContainer(Version.POSTGRES_VERSION)
            .withStartupTimeout(Duration.ofMinutes(5));

    @LocalServerPort
    protected int port;

    @Autowired
    private PostgresBalanceChangesRepository postgresBalanceChangesRepository;

    @Autowired
    private PartyDao partyDao;

    @Autowired
    private ShopDao shopDao;

    @Autowired
    private JdbcTemplate postgresJdbcTemplate;

    @Test
    public void testCount() {
        postgresBalanceChangesRepository.insertPayments(List.of(payment()));
        postgresBalanceChangesRepository.insertRefunds(List.of(refund()));
        postgresBalanceChangesRepository.insertAdjustments(List.of(adjustment()));
        postgresBalanceChangesRepository.insertPayouts(List.of(payout()));

        long count = postgresJdbcTemplate.queryForObject(
                "SELECT count(*) AS count FROM analytics.balance_change",
                (resultSet, i) -> resultSet.getLong("count"));

        assertEquals(4L, count);
    }

    @Test
    public void testPartySave() {
        Party party = EnhancedRandom.random(Party.class);
        partyDao.saveParty(party);
        Party savedParty = partyDao.getPartyById(party.getPartyId());
        Assert.assertEquals(party, savedParty);
    }

    @Test
    public void testShopSave() {
        Shop shop = EnhancedRandom.random(Shop.class);
        shopDao.saveShop(shop);
        Shop savedShop = shopDao.getShopByPartyIdAndShopId(shop.getPartyId(), shop.getShopId());
        Assert.assertEquals(shop, savedShop);
    }

    @Test
    public void testDuplicatePartySave() {
        Party firstParty = EnhancedRandom.random(Party.class);
        partyDao.saveParty(firstParty);
        Party secondParty = EnhancedRandom.random(Party.class);
        secondParty.setPartyId(firstParty.getPartyId());
        partyDao.saveParty(secondParty);
        Party savedParty = partyDao.getPartyById(secondParty.getPartyId());
        Assert.assertEquals(secondParty, savedParty);
    }

    @Test
    public void testDuplicateShopSave() {
        Shop firstShop = EnhancedRandom.random(Shop.class);
        shopDao.saveShop(firstShop);
        Shop secondShop = EnhancedRandom.random(Shop.class);
        secondShop.setPartyId(firstShop.getPartyId());
        secondShop.setShopId(firstShop.getShopId());
        shopDao.saveShop(secondShop);
        Shop savedShop = shopDao.getShopByPartyIdAndShopId(secondShop.getPartyId(), secondShop.getShopId());
        Assert.assertEquals(secondShop, savedShop);
    }

    private PaymentRow payment() {
        PaymentRow paymentRow = new PaymentRow();
        paymentRow.setInvoiceId("invoice_id");
        paymentRow.setSequenceId(1L);
        paymentRow.setEventTime(LocalDateTime.ofInstant(Instant.now(), ZoneOffset.UTC));
        paymentRow.setCurrency("RUB");
        paymentRow.setPartyId("party_id");
        paymentRow.setShopId("shop_id");
        paymentRow.setCashFlowResult(CashFlowResult.builder()
                .amount(1000L)
                .systemFee(100L)
                .build());
        return paymentRow;
    }

    private RefundRow refund() {
        RefundRow refundRow = new RefundRow();
        refundRow.setInvoiceId("invoice_id");
        refundRow.setSequenceId(2L);
        refundRow.setEventTime(LocalDateTime.ofInstant(Instant.now(), ZoneOffset.UTC));
        refundRow.setCurrency("RUB");
        refundRow.setPartyId("party_id");
        refundRow.setShopId("shop_id");
        refundRow.setCashFlowResult(CashFlowResult.builder()
                .amount(500L)
                .systemFee(50L)
                .build());
        return refundRow;
    }

    private AdjustmentRow adjustment() {
        AdjustmentRow adjustmentRow = new AdjustmentRow();
        adjustmentRow.setInvoiceId("invoice_id");
        adjustmentRow.setSequenceId(3L);
        adjustmentRow.setEventTime(LocalDateTime.ofInstant(Instant.now(), ZoneOffset.UTC));
        adjustmentRow.setCurrency("RUB");
        adjustmentRow.setPartyId("party_id");
        adjustmentRow.setShopId("shop_id");
        adjustmentRow.setCashFlowResult(CashFlowResult.builder()
                .systemFee(250L)
                .build());
        adjustmentRow.setOldCashFlowResult(CashFlowResult.builder()
                .systemFee(100L)
                .build());
        return adjustmentRow;
    }

    private PayoutRow payout() {
        PayoutRow payoutRow = new PayoutRow();
        payoutRow.setPayoutId("pauout_id");
        payoutRow.setStatus(PayoutStatus.paid);
        payoutRow.setEventTime(LocalDateTime.ofInstant(Instant.now(), ZoneOffset.UTC));
        payoutRow.setCurrency("RUB");
        payoutRow.setPartyId("party_id");
        payoutRow.setShopId("shop_id");
        payoutRow.setAmount(10_000L);
        payoutRow.setFee(1000L);
        return payoutRow;
    }

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

}
