package com.rbkmoney.analytics.listener;

import com.rbkmoney.analytics.AnalyticsApplication;
import com.rbkmoney.analytics.dao.repository.postgres.party.management.ContractorDao;
import com.rbkmoney.analytics.dao.repository.postgres.party.management.PartyDao;
import com.rbkmoney.analytics.dao.repository.postgres.party.management.ShopDao;
import com.rbkmoney.analytics.domain.db.enums.ContractorType;
import com.rbkmoney.analytics.domain.db.enums.Suspension;
import com.rbkmoney.analytics.domain.db.tables.pojos.Contractor;
import com.rbkmoney.analytics.domain.db.tables.pojos.Party;
import com.rbkmoney.analytics.domain.db.tables.pojos.Shop;
import com.rbkmoney.analytics.utils.KafkaAbstractTest;
import com.rbkmoney.analytics.utils.PartyFlowGenerator;
import com.rbkmoney.damsel.domain.PartyContractor;
import com.rbkmoney.damsel.domain.RussianLegalEntity;
import com.rbkmoney.machinegun.eventsink.SinkEvent;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
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
import org.testcontainers.containers.PostgreSQLContainer;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static com.rbkmoney.analytics.utils.PartyFlowGenerator.CONTRACTOR_ID;
import static com.rbkmoney.analytics.utils.PartyFlowGenerator.SETTLEMENT_ID;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.*;

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
            postgres.start();
        }
    }

    @Autowired
    private PartyDao partyDao;

    @Autowired
    private ShopDao shopDao;

    @Autowired
    private ContractorDao contractorDao;

    @Autowired
    private JdbcTemplate postgresJdbcTemplate;

    @Before
    public void clean(){
        clearDb();
    }

    @Test
    // This flow show forward case: party events - contractor - contract - shop
    public void testPartyEventSink() throws IOException {
        String partyId = UUID.randomUUID().toString();
        String shopId = UUID.randomUUID().toString();
        List<SinkEvent> sinkEvents = PartyFlowGenerator.generatePartyFlow(partyId, shopId);

        sinkEvents.forEach(event -> produceMessageToTopic(this.partyTopic, event));

        await().atMost(60, SECONDS).until(() -> {
            Integer partyCount = postgresJdbcTemplate.queryForObject("SELECT count(*) FROM analytics.contractor" +
                    " WHERE contractor_identification_level = 'partial'", Integer.class);
            Integer shopCount = postgresJdbcTemplate.queryForObject(String.format("SELECT count(*) FROM analytics.shop" +
                    " WHERE account_settlement = '%s'", SETTLEMENT_ID), Integer.class);
            return checkResult(partyCount) && checkResult(shopCount);
        });
    }

    @Test
    // This flow show only party and contractor flow
    public void testPartyFlowSave() throws IOException, InterruptedException {
        String partyId = UUID.randomUUID().toString();
        List<SinkEvent> sinkEvents = PartyFlowGenerator.generatePartyContractorFlow(partyId);

        sinkEvents.forEach(event -> produceMessageToTopic(this.partyTopic, event));

        await().atMost(60, SECONDS).until(() -> {
            Party party = partyDao.getPartyById(partyId);
            if (party == null) {
                Thread.sleep(1000);
                return false;
            }
            Integer partyCount = postgresJdbcTemplate.queryForObject("SELECT count(*) FROM analytics.contractor" +
                    " WHERE contractor_identification_level = 'partial'", Integer.class);
            return checkResult(partyCount);
        });

        Party party = partyDao.getPartyById(partyId);
        assertFalse(party.getPartyId().isEmpty());
        assertEquals(PartyFlowGenerator.PARTY_BLOCK_REASON, party.getBlockedReason());
        assertNotNull(party.getBlockedSince());
        assertEquals(Suspension.active, party.getSuspension());
        assertEquals(PartyFlowGenerator.PARTY_REVISION_ID.toString(), party.getRevisionId());
        assertEquals(PartyFlowGenerator.PARTY_EMAIL, party.getEmail());
    }

    @NotNull
    private Boolean checkResult(Integer count) throws InterruptedException {
        boolean notEmpty = count != null && count > 0;
        if (notEmpty) {
            return true;
        }
        Thread.sleep(1000);
        return false;
    }

    @Test
    // old flow when contract and shop come before contractor
    public void testShopFlowSave() throws IOException {
        String partyId = UUID.randomUUID().toString();
        String shopId = UUID.randomUUID().toString();
        List<SinkEvent> sinkEvents = PartyFlowGenerator.generateShopFlow(partyId, shopId);
        sinkEvents.forEach(event -> produceMessageToTopic(this.partyTopic, event));
        await().atMost(60, SECONDS).until(() -> {
            Integer lastShopCount = postgresJdbcTemplate.queryForObject(String.format(
                    "SELECT count(*) FROM analytics.shop WHERE shop_id = '%s' AND suspension = 'suspended' AND contractor_type='legal_entity'", shopId), Integer.class);
            if (lastShopCount <= 0) {
                Thread.sleep(1000);
                return false;
            }
            return true;
        });

        Shop shop = shopDao.getShopByPartyIdAndShopId(partyId, shopId);
        assertEquals(partyId, shop.getPartyId());
        assertEquals(shopId, shop.getShopId());
        assertEquals(PartyFlowGenerator.CURRENCY_SYMBOL, shop.getAccountCurrencyCode());
        assertFalse(shop.getAccountGuarantee().isEmpty());
        assertEquals(PartyFlowGenerator.SHOP_UNBLOCK_REASON, shop.getUnblockedReason());
        assertNotNull(shop.getUnblockedSince());
        assertEquals(Suspension.suspended, shop.getSuspension());
        assertNotNull(shop.getSuspensionSuspendedSince());
        assertNotNull(shop.getSuspensionActiveSince());
        assertEquals(PartyFlowGenerator.CATEGORY_ID, shop.getCategoryId());
        assertEquals(PartyFlowGenerator.DETAILS_NAME, shop.getDetailsName());
        assertEquals(PartyFlowGenerator.DETAILS_DESCRIPTION, shop.getDetailsDescription());
        assertEquals(PartyFlowGenerator.SCHEDULE_ID, shop.getPayoutScheduleId());
        assertEquals(PartyFlowGenerator.PAYOUT_TOOL_ID, shop.getPayoutToolId());
        assertEquals(PartyFlowGenerator.SHOP_ACCOUNT_PAYOUT.toString(), shop.getAccountPayout());
        assertEquals(String.valueOf(SETTLEMENT_ID), shop.getAccountSettlement());
        assertEquals(ContractorType.legal_entity, shop.getContractorType());
        assertNotNull(shop.getRussianLegalEntityActualAddress());
        assertNotNull(shop.getRussianLegalEntityBankAccount());
        assertNotNull(shop.getRussianLegalEntityBankBik());
        assertNotNull(shop.getRussianLegalEntityBankName());
        assertNotNull(shop.getRussianLegalEntityBankPostAccount());
        assertNotNull(shop.getRussianLegalEntityInn());
        assertNotNull(shop.getRussianLegalEntityName());
        assertNotNull(shop.getRussianLegalEntityPostAddress());
        assertNotNull(shop.getRussianLegalEntityRegisteredNumber());
        assertNotNull(shop.getRussianLegalEntityRepresentativeDocument());
        assertNotNull(shop.getRussianLegalEntityRepresentativeFullName());
        assertNotNull(shop.getRussianLegalEntityRepresentativePosition());
    }

    @Test
    //multi events flow
    public void testMultiplePartySave() throws IOException, InterruptedException {
        Integer count = 3;
        String lastPartyId = UUID.randomUUID().toString();
        String lastShopId = UUID.randomUUID().toString();

        PartyContractor partyContractor = PartyFlowGenerator.buildRussianLegalPartyContractor();
        RussianLegalEntity russianLegalEntity = partyContractor.getContractor().getLegalEntity().getRussianLegalEntity();
        List<SinkEvent> sinkEvents = PartyFlowGenerator.generatePartyFlowWithCount(count, lastPartyId, lastShopId, partyContractor);
        sinkEvents.forEach(event -> produceMessageToTopic(this.partyTopic, event));
        await().atMost(60, SECONDS).until(() -> {
            Integer lastShopCount = postgresJdbcTemplate.queryForObject(String.format(
                    "SELECT count(*) FROM analytics.contractor WHERE contractor_id = '%s'",
                    2, russianLegalEntity.getInn()), Integer.class);
            if (lastShopCount <= 0) {
                Thread.sleep(1000);
                return false;
            }
            return true;
        });

        checkContractorFields(russianLegalEntity);

        checkShopFields(russianLegalEntity, lastPartyId, lastShopId);
    }

    private void clearDb() {
        postgresJdbcTemplate.execute("TRUNCATE TABLE analytics.contractor;");
        postgresJdbcTemplate.execute("TRUNCATE TABLE analytics.party;");
        postgresJdbcTemplate.execute("TRUNCATE TABLE analytics.shop;");
        postgresJdbcTemplate.execute("TRUNCATE TABLE analytics.contract;");
    }

    private void checkContractorFields(RussianLegalEntity russianLegalEntity) {
        Contractor contractorForUpdate = contractorDao.getContractorById(CONTRACTOR_ID);
        assertEquals(russianLegalEntity.getInn(), contractorForUpdate.getRussianLegalEntityInn());
        assertEquals(russianLegalEntity.getActualAddress(), contractorForUpdate.getRussianLegalEntityActualAddress());
        assertEquals(russianLegalEntity.getRussianBankAccount().getAccount(), contractorForUpdate.getRussianLegalEntityBankAccount());
        assertEquals(russianLegalEntity.getRussianBankAccount().getBankBik(), contractorForUpdate.getRussianLegalEntityBankBik());
        assertEquals(russianLegalEntity.getRussianBankAccount().getBankName(), contractorForUpdate.getRussianLegalEntityBankName());
        assertEquals(russianLegalEntity.getRussianBankAccount().getBankPostAccount(), contractorForUpdate.getRussianLegalEntityBankPostAccount());
        assertEquals(russianLegalEntity.getRegisteredName(), contractorForUpdate.getRussianLegalEntityName());
        assertEquals(russianLegalEntity.getPostAddress(), contractorForUpdate.getRussianLegalEntityPostAddress());
        assertEquals(russianLegalEntity.getRegisteredNumber(), contractorForUpdate.getRussianLegalEntityRegisteredNumber());
        assertEquals(russianLegalEntity.getRepresentativeDocument(), contractorForUpdate.getRussianLegalEntityRepresentativeDocument());
        assertEquals(russianLegalEntity.getRepresentativeFullName(), contractorForUpdate.getRussianLegalEntityRepresentativeFullName());
        assertEquals(russianLegalEntity.getRepresentativePosition(), contractorForUpdate.getRussianLegalEntityRepresentativePosition());
    }

    private void checkShopFields(RussianLegalEntity russianLegalEntity, String partyId, String shopId) {
        Shop contractorForUpdate = shopDao.getShopByPartyIdAndShopId(partyId, shopId);
        assertEquals(russianLegalEntity.getInn(), contractorForUpdate.getRussianLegalEntityInn());
        assertEquals(russianLegalEntity.getActualAddress(), contractorForUpdate.getRussianLegalEntityActualAddress());
        assertEquals(russianLegalEntity.getRussianBankAccount().getAccount(), contractorForUpdate.getRussianLegalEntityBankAccount());
        assertEquals(russianLegalEntity.getRussianBankAccount().getBankBik(), contractorForUpdate.getRussianLegalEntityBankBik());
        assertEquals(russianLegalEntity.getRussianBankAccount().getBankName(), contractorForUpdate.getRussianLegalEntityBankName());
        assertEquals(russianLegalEntity.getRussianBankAccount().getBankPostAccount(), contractorForUpdate.getRussianLegalEntityBankPostAccount());
        assertEquals(russianLegalEntity.getRegisteredName(), contractorForUpdate.getRussianLegalEntityName());
        assertEquals(russianLegalEntity.getPostAddress(), contractorForUpdate.getRussianLegalEntityPostAddress());
        assertEquals(russianLegalEntity.getRegisteredNumber(), contractorForUpdate.getRussianLegalEntityRegisteredNumber());
        assertEquals(russianLegalEntity.getRepresentativeDocument(), contractorForUpdate.getRussianLegalEntityRepresentativeDocument());
        assertEquals(russianLegalEntity.getRepresentativeFullName(), contractorForUpdate.getRussianLegalEntityRepresentativeFullName());
        assertEquals(russianLegalEntity.getRepresentativePosition(), contractorForUpdate.getRussianLegalEntityRepresentativePosition());
    }
}
