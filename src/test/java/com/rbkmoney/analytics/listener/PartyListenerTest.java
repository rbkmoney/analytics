package com.rbkmoney.analytics.listener;

import com.rbkmoney.analytics.AnalyticsApplication;
import com.rbkmoney.analytics.dao.repository.postgres.PostgresPartyDao;
import com.rbkmoney.analytics.domain.db.enums.Contractor;
import com.rbkmoney.analytics.domain.db.enums.ContractorIdentificationLvl;
import com.rbkmoney.analytics.domain.db.enums.LegalEntity;
import com.rbkmoney.analytics.domain.db.tables.pojos.Party;
import com.rbkmoney.analytics.domain.db.tables.pojos.Shop;
import com.rbkmoney.analytics.utils.FileUtil;
import com.rbkmoney.machinegun.eventsink.SinkEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.testcontainers.containers.ClickHouseContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;

import static com.rbkmoney.analytics.listener.PartyFlowGenerator.SETTLEMENT_ID;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

@Slf4j
@RunWith(SpringRunner.class)
@SpringBootTest(classes = AnalyticsApplication.class,
        properties = {"kafka.state.cache.size=0"})
@ContextConfiguration(initializers = {PartyListenerTest.Initializer.class})
public class PartyListenerTest extends KafkaAbstractTest {

    @ClassRule
    @SuppressWarnings("rawtypes")
    public static PostgreSQLContainer postgres = (PostgreSQLContainer) new PostgreSQLContainer("postgres:9.6")
            .withStartupTimeout(Duration.ofMinutes(5));

    public static class Initializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {
        @Override
        public void initialize(ConfigurableApplicationContext configurableApplicationContext) {
            TestPropertyValues.of(
                    "postgres.db.url=" + postgres.getJdbcUrl(),
                    "postgres.db.user=" + postgres.getUsername(),
                    "postgres.db.password=" + postgres.getPassword(),
                    "spring.flyway.url=" + postgres.getJdbcUrl(),
                    "spring.flyway.user=" + postgres.getUsername(),
                    "spring.flyway.password=" + postgres.getPassword(),
                    "spring.flyway.enabled=true")
                    .applyTo(configurableApplicationContext.getEnvironment());
        }
    }

    @Autowired
    private PostgresPartyDao postgresPartyDao;

    @Autowired
    private JdbcTemplate postgresJdbcTemplate;

    @Test
    public void testPartyEventSink() throws IOException {
        List<SinkEvent> sinkEvents = PartyFlowGenerator.generatePartyFlow();

        sinkEvents.forEach(this::produceMessageToParty);

        await().atMost(60, SECONDS).until(() -> {
            Integer partyCount = postgresJdbcTemplate.queryForObject("SELECT count(*) FROM analytics.party" +
                    " WHERE contractor_identification_level = 'partial'", Integer.class);
            boolean isLastPartyChange = partyCount != null && partyCount > 0;
            Integer shopCount = postgresJdbcTemplate.queryForObject(String.format("SELECT count(*) FROM analytics.shop" +
                    " WHERE account_settlement = '%s'", SETTLEMENT_ID), Integer.class);
            boolean isLastShopChange = shopCount != null && shopCount > 0;
            if (isLastPartyChange && isLastShopChange) {
                return true;
            }
            Thread.sleep(1000);
            return false;
        });
    }

    @Test
    public void testPartyFlowSave() throws IOException {
        List<SinkEvent> sinkEvents = PartyFlowGenerator.generatePartyContractorFlow();

        sinkEvents.forEach(this::produceMessageToParty);

        await().atMost(60, SECONDS).until(() -> {
            Party party = postgresPartyDao.getParty(PartyFlowGenerator.PARTY_ID);
            if (party == null) {
                Thread.sleep(1000);
                return false;
            }
            return party.getContractorIdentificationLevel() == ContractorIdentificationLvl.partial;
        });

        Party party = postgresPartyDao.getParty(PartyFlowGenerator.PARTY_ID);
        Assert.assertFalse(party.getPartyId().isEmpty());
        Assert.assertEquals(PartyFlowGenerator.PARTY_BLOCK_REASON, party.getBlockedReason());
        Assert.assertNotNull(party.getBlockedSince());
        Assert.assertEquals("active", party.getSuspension().name());
        Assert.assertEquals(String.valueOf(PartyFlowGenerator.PARTY_REVISION_ID), party.getRevisionId());
        Assert.assertEquals(PartyFlowGenerator.PARTY_EMAIL, party.getEmail());
        Assert.assertEquals(Contractor.legal_entity, party.getContractorType());
        Assert.assertEquals(LegalEntity.russian_legal_entity, party.getLegalEntityType());
        Assert.assertNotNull(party.getRussianLegalEntityActualAddress());
        Assert.assertNotNull(party.getRussianLegalEntityBankAccount());
        Assert.assertNotNull(party.getRussianLegalEntityBankBik());
        Assert.assertNotNull(party.getRussianLegalEntityBankName());
        Assert.assertNotNull(party.getRussianLegalEntityBankPostAccount());
        Assert.assertNotNull(party.getRussianLegalEntityInn());
        Assert.assertNotNull(party.getRussianLegalEntityName());
        Assert.assertNotNull(party.getRussianLegalEntityPostAddress());
        Assert.assertNotNull(party.getRussianLegalEntityRegisteredNumber());
        Assert.assertNotNull(party.getRussianLegalEntityRepresentativeDocument());
        Assert.assertNotNull(party.getRussianLegalEntityRepresentativeFullName());
        Assert.assertNotNull(party.getRussianLegalEntityRepresentativePosition());
    }

    @Test
    public void testShopFlowSave() throws IOException {
        List<SinkEvent> sinkEvents = PartyFlowGenerator.generateShopFlow();
        sinkEvents.forEach(this::produceMessageToParty);
        await().atMost(60, SECONDS).until(() -> {
            Shop shop = postgresPartyDao.getShop(PartyFlowGenerator.PARTY_ID, PartyFlowGenerator.SHOP_ID);
            if (shop == null) {
                Thread.sleep(1000);
                return false;
            }
            return shop.getAccountCurrencyCode().equals(PartyFlowGenerator.CURRENCY_SYMBOL);
        });
        Shop shop = postgresPartyDao.getShop(PartyFlowGenerator.PARTY_ID, PartyFlowGenerator.SHOP_ID);
        Assert.assertEquals(PartyFlowGenerator.PARTY_ID, shop.getPartyId());
        Assert.assertEquals(PartyFlowGenerator.SHOP_ID, shop.getShopId());
        Assert.assertEquals(PartyFlowGenerator.CURRENCY_SYMBOL, shop.getAccountCurrencyCode());
        Assert.assertFalse(shop.getAccountGuarantee().isEmpty());
        Assert.assertEquals(PartyFlowGenerator.SHOP_UNBLOCK_REASON, shop.getUnblockedReason());
        Assert.assertNotNull(shop.getUnblockedSince());
        Assert.assertEquals("active", shop.getSuspension().name());
        Assert.assertEquals(PartyFlowGenerator.CATEGORY_ID, shop.getCategoryId());
        Assert.assertEquals(PartyFlowGenerator.DETAILS_NAME, shop.getDetailsName());
        Assert.assertEquals(PartyFlowGenerator.DETAILS_DESCRIPTION, shop.getDetailsDescription());
        Assert.assertEquals(PartyFlowGenerator.SCHEDULE_ID, shop.getPayoutScheduleId());
        Assert.assertEquals(PartyFlowGenerator.PAYOUT_TOOL_ID, shop.getPayoutToolId());
        Assert.assertEquals(String.valueOf(SETTLEMENT_ID), shop.getAccountSettlement());
    }

}
