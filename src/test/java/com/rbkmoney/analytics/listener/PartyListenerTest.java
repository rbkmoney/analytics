package com.rbkmoney.analytics.listener;

import com.rbkmoney.analytics.AnalyticsApplication;
import com.rbkmoney.analytics.dao.repository.postgres.party.management.ContractorDao;
import com.rbkmoney.analytics.dao.repository.postgres.party.management.PartyDao;
import com.rbkmoney.analytics.dao.repository.postgres.party.management.ShopDao;
import com.rbkmoney.analytics.domain.db.enums.ContractorType;
import com.rbkmoney.analytics.domain.db.enums.LegalEntity;
import com.rbkmoney.analytics.domain.db.enums.Suspension;
import com.rbkmoney.analytics.domain.db.tables.pojos.Contractor;
import com.rbkmoney.analytics.domain.db.tables.pojos.Party;
import com.rbkmoney.analytics.domain.db.tables.pojos.Shop;
import com.rbkmoney.analytics.utils.KafkaAbstractTest;
import com.rbkmoney.analytics.utils.PartyFlowGenerator;
import com.rbkmoney.damsel.domain.PartyContractor;
import com.rbkmoney.damsel.domain.RussianLegalEntity;
import com.rbkmoney.damsel.payment_processing.PartyChange;
import com.rbkmoney.machinegun.eventsink.SinkEvent;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
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

import static com.rbkmoney.analytics.utils.PartyFlowGenerator.*;
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

    @Test
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
    public void testPartyFlowSave() throws IOException {
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
        Assert.assertFalse(party.getPartyId().isEmpty());
        Assert.assertEquals(PartyFlowGenerator.PARTY_BLOCK_REASON, party.getBlockedReason());
        Assert.assertNotNull(party.getBlockedSince());
        Assert.assertEquals(Suspension.active, party.getSuspension());
        Assert.assertEquals(PartyFlowGenerator.PARTY_REVISION_ID.toString(), party.getRevisionId());
        Assert.assertEquals(PartyFlowGenerator.PARTY_EMAIL, party.getEmail());
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
    public void testShopFlowSave() throws IOException {
        String partyId = UUID.randomUUID().toString();
        String shopId = UUID.randomUUID().toString();
        List<SinkEvent> sinkEvents = PartyFlowGenerator.generateShopFlow(partyId, shopId);
        sinkEvents.forEach(event -> produceMessageToTopic(this.partyTopic, event));
        await().atMost(60, SECONDS).until(() -> {
            Integer lastShopCount = postgresJdbcTemplate.queryForObject(String.format(
                    "SELECT count(*) FROM analytics.shop WHERE shop_id = '%s' AND suspension = 'suspended'", shopId), Integer.class);
            if (lastShopCount <= 0) {
                Thread.sleep(1000);
                return false;
            }
            return true;
        });

        Shop shop = shopDao.getShopByPartyIdAndShopId(partyId, shopId);
        Assert.assertEquals(partyId, shop.getPartyId());
        Assert.assertEquals(shopId, shop.getShopId());
        Assert.assertEquals(PartyFlowGenerator.CURRENCY_SYMBOL, shop.getAccountCurrencyCode());
        Assert.assertFalse(shop.getAccountGuarantee().isEmpty());
        Assert.assertEquals(PartyFlowGenerator.SHOP_UNBLOCK_REASON, shop.getUnblockedReason());
        Assert.assertNotNull(shop.getUnblockedSince());
        Assert.assertEquals(Suspension.suspended, shop.getSuspension());
        Assert.assertNotNull(shop.getSuspensionSuspendedSince());
        Assert.assertNotNull(shop.getSuspensionActiveSince());
        Assert.assertEquals(PartyFlowGenerator.CATEGORY_ID, shop.getCategoryId());
        Assert.assertEquals(PartyFlowGenerator.DETAILS_NAME, shop.getDetailsName());
        Assert.assertEquals(PartyFlowGenerator.DETAILS_DESCRIPTION, shop.getDetailsDescription());
        Assert.assertEquals(PartyFlowGenerator.SCHEDULE_ID, shop.getPayoutScheduleId());
        Assert.assertEquals(PartyFlowGenerator.PAYOUT_TOOL_ID, shop.getPayoutToolId());
        Assert.assertEquals(PartyFlowGenerator.SHOP_ACCOUNT_PAYOUT.toString(), shop.getAccountPayout());
        Assert.assertEquals(String.valueOf(SETTLEMENT_ID), shop.getAccountSettlement());
        Assert.assertEquals(ContractorType.legal_entity, shop.getContractorType());
        Assert.assertNotNull(shop.getRussianLegalEntityActualAddress());
        Assert.assertNotNull(shop.getRussianLegalEntityBankAccount());
        Assert.assertNotNull(shop.getRussianLegalEntityBankBik());
        Assert.assertNotNull(shop.getRussianLegalEntityBankName());
        Assert.assertNotNull(shop.getRussianLegalEntityBankPostAccount());
        Assert.assertNotNull(shop.getRussianLegalEntityInn());
        Assert.assertNotNull(shop.getRussianLegalEntityName());
        Assert.assertNotNull(shop.getRussianLegalEntityPostAddress());
        Assert.assertNotNull(shop.getRussianLegalEntityRegisteredNumber());
        Assert.assertNotNull(shop.getRussianLegalEntityRepresentativeDocument());
        Assert.assertNotNull(shop.getRussianLegalEntityRepresentativeFullName());
        Assert.assertNotNull(shop.getRussianLegalEntityRepresentativePosition());
    }

    @Test
    public void testMultiplePartySave() throws IOException {
        Integer count = 50;
        String lastPartyId = UUID.randomUUID().toString();
        String lastShopId = UUID.randomUUID().toString();

        PartyContractor partyContractor = PartyFlowGenerator.buildRussianLegalPartyContractor();
        RussianLegalEntity russianLegalEntity = partyContractor.getContractor().getLegalEntity().getRussianLegalEntity();
        List<SinkEvent> sinkEvents = PartyFlowGenerator.generatePartyFlowWithCount(count, lastPartyId, lastShopId, partyContractor);
        sinkEvents.forEach(event -> produceMessageToTopic(this.partyTopic, event));
        await().atMost(120, SECONDS).until(() -> {
            Integer partyCount = postgresJdbcTemplate.queryForObject("SELECT count(*) FROM analytics.party", Integer.class);
            if (partyCount < count) {
                Thread.sleep(1000);
                return false;
            }
            Integer lastShopCount = postgresJdbcTemplate.queryForObject(String.format(
                    "SELECT count(*) FROM analytics.shop WHERE shop_id = '%s' AND russian_legal_entity_inn = '%s' ",
                    lastShopId, russianLegalEntity.getInn()), Integer.class);
            if (lastShopCount <= 0) {
                Thread.sleep(1000);
                return false;
            }
            return true;
        });

        checkContractorFields(russianLegalEntity);

        checkShopFields(russianLegalEntity, lastPartyId, lastShopId);
    }

    @Test
    public void testMachineEventWithMultiplePartyChange() throws IOException {
        Integer count = 10;
        String partyId = UUID.randomUUID().toString();
        PartyContractor partyContractor = PartyFlowGenerator.buildRussianLegalPartyContractor();
        RussianLegalEntity russianLegalEntity = partyContractor.getContractor().getLegalEntity().getRussianLegalEntity();
        List<SinkEvent> sinkEvents = PartyFlowGenerator.generatePartyFlowWithMultiplePartyChange(count, partyId, partyContractor);
        sinkEvents.forEach(event -> produceMessageToTopic(this.partyTopic, event));
        await().atMost(60, SECONDS).until(() -> {
            Integer partyCount = postgresJdbcTemplate.queryForObject("SELECT count(*) FROM analytics.party", Integer.class);
            if (partyCount < count) {
                Thread.sleep(1000);
                return false;
            }
            Integer lastPartyCount = postgresJdbcTemplate.queryForObject(String.format(
                    "SELECT count(*) FROM analytics.contractor WHERE party_id = '%s' and russian_legal_entity_inn = '%s'", partyId, russianLegalEntity.getInn()), Integer.class);
            if (lastPartyCount <= 0) {
                Thread.sleep(1000);
                return false;
            }
            return true;
        });

        checkContractorFields(russianLegalEntity);
    }

    @Test
    public void testMachineEventWithMultiplePartyShopChange() throws IOException {
        Integer count = 10;
        String partyId = UUID.randomUUID().toString();
        String shopId = UUID.randomUUID().toString();
        String contractId = UUID.randomUUID().toString();

        String payoutToolId = "testPayoutToolId";
        PartyChange shopPayoutToolChangedPartyChange = buildShopPayoutToolChangedPartyChange(shopId, payoutToolId);
        List<SinkEvent> sinkEvents = PartyFlowGenerator.generatePartyFlowWithMultiplePartyShopChange(count, partyId, shopId, contractId, shopPayoutToolChangedPartyChange);
        sinkEvents.forEach(event -> produceMessageToTopic(this.partyTopic, event));
        await().atMost(60, SECONDS).until(() -> {
            Integer shopCount = postgresJdbcTemplate.queryForObject("SELECT count(*) FROM analytics.shop", Integer.class);
            if (shopCount < count) {
                Thread.sleep(1000);
                return false;
            }
            Integer lastShopCount = postgresJdbcTemplate.queryForObject(String.format(
                    "SELECT count(*) FROM analytics.shop WHERE party_id = '%s' and shop_id = '%s' and payout_tool_id = '%s'", partyId, shopId, payoutToolId), Integer.class);
            if (lastShopCount <= 0) {
                Thread.sleep(1000);
                return false;
            }
            return true;
        });
        Shop shop = shopDao.getShopByPartyIdAndShopId(partyId, shopId);
        Assert.assertNotNull(shop.getShopId());
        Assert.assertNotNull(shop.getPartyId());
        Assert.assertNotNull(shop.getSuspensionActiveSince());
        Assert.assertNotNull(shop.getSuspension());
        Assert.assertNotNull(shop.getCategoryId());
        Assert.assertNotNull(shop.getUnblockedSince());
        Assert.assertNotNull(shop.getAccountCurrencyCode());
        Assert.assertNotNull(shop.getAccountGuarantee());
        Assert.assertNotNull(shop.getAccountPayout());
        Assert.assertNotNull(shop.getAccountSettlement());
        Assert.assertNotNull(shop.getDetailsDescription());
        Assert.assertNotNull(shop.getDetailsName());
        Assert.assertNotNull(shop.getDetailsDescription());
        Assert.assertNotNull(shop.getPayoutScheduleId());
        Assert.assertNotNull(shop.getBlocking());
        Assert.assertNotNull(shop.getBlockedSince());
        Assert.assertNotNull(shop.getBlockedReason());
        Assert.assertNotNull(shop.getContractId());
        Assert.assertNotNull(shop.getCreatedAt());
        Assert.assertNotNull(shop.getLocationUrl());
        Assert.assertNotNull(shop.getEventId());
        Assert.assertNotNull(shop.getEventTime());
        Assert.assertEquals(payoutToolId, shop.getPayoutToolId());
    }

    @Test
    public void testMachineEventWithMultipleShopInOneChange() throws IOException {
        String partyId = UUID.randomUUID().toString();
        String shopId = UUID.randomUUID().toString();
        String contractId = UUID.randomUUID().toString();

        String payoutToolId = "testPayoutToolId";
        PartyChange shopPayoutToolChangedPartyChange = buildShopPayoutToolChangedPartyChange(shopId, payoutToolId);
        List<SinkEvent> sinkEvents = PartyFlowGenerator.generatePartyFlowWithMultipleShopInOneChange(partyId, shopId, contractId, shopPayoutToolChangedPartyChange);
        sinkEvents.forEach(event -> produceMessageToTopic(this.partyTopic, event));
        await().atMost(60, SECONDS).until(() -> {
            Integer lastShopCount = postgresJdbcTemplate.queryForObject(String.format(
                    "SELECT count(*) FROM analytics.shop WHERE party_id = '%s' and shop_id = '%s' and payout_tool_id = '%s'", partyId, shopId, payoutToolId), Integer.class);
            if (lastShopCount <= 0) {
                Thread.sleep(1000);
                return false;
            }
            return true;
        });
        Shop shop = shopDao.getShopByPartyIdAndShopId(partyId, shopId);
        Assert.assertNotNull(shop.getShopId());
        Assert.assertNotNull(shop.getPartyId());
        Assert.assertNotNull(shop.getCreatedAt());
        Assert.assertNotNull(shop.getBlocking());
        Assert.assertNotNull(shop.getBlockedSince());
        Assert.assertNotNull(shop.getBlockedReason());
        Assert.assertNotNull(shop.getAccountCurrencyCode());
        Assert.assertNotNull(shop.getAccountGuarantee());
        Assert.assertNotNull(shop.getAccountPayout());
        Assert.assertNotNull(shop.getAccountSettlement());
        Assert.assertNotNull(shop.getPayoutScheduleId());
        Assert.assertNotNull(shop.getEventId());
        Assert.assertNotNull(shop.getEventTime());
        Assert.assertEquals(payoutToolId, shop.getPayoutToolId());
    }

    @Test
    public void testCotractWithContractorSave() throws IOException {
        String partyId = UUID.randomUUID().toString();
        com.rbkmoney.damsel.domain.LegalEntity legalEntity = new com.rbkmoney.damsel.domain.LegalEntity();
        RussianLegalEntity russianLegalEntity = PartyFlowGenerator.buildRussianLegalEntity();
        legalEntity.setRussianLegalEntity(russianLegalEntity);
        List<SinkEvent> sinkEvents = generatePartyFlowWithContract(partyId, legalEntity);
        sinkEvents.forEach(event -> produceMessageToTopic(this.partyTopic, event));
        await().atMost(60, SECONDS).until(() -> {
            Integer lastPartyCount = postgresJdbcTemplate.queryForObject(String.format(
                    "SELECT count(*) FROM analytics.contractor "
                            + "WHERE party_id = '%s' and russian_legal_entity_inn = '%s'",
                    partyId, russianLegalEntity.getInn()), Integer.class);
            if (lastPartyCount <= 0) {
                Thread.sleep(1000);
                return false;
            }
            return true;
        });

        checkContractorFields(russianLegalEntity);
    }

    private void checkContractorFields(RussianLegalEntity russianLegalEntity) {
        Contractor contractorForUpdate = contractorDao.getContractorById(CONTRACTOR_ID);
        Assert.assertEquals(russianLegalEntity.getInn(), contractorForUpdate.getRussianLegalEntityInn());
        Assert.assertEquals(russianLegalEntity.getActualAddress(), contractorForUpdate.getRussianLegalEntityActualAddress());
        Assert.assertEquals(russianLegalEntity.getRussianBankAccount().getAccount(), contractorForUpdate.getRussianLegalEntityBankAccount());
        Assert.assertEquals(russianLegalEntity.getRussianBankAccount().getBankBik(), contractorForUpdate.getRussianLegalEntityBankBik());
        Assert.assertEquals(russianLegalEntity.getRussianBankAccount().getBankName(), contractorForUpdate.getRussianLegalEntityBankName());
        Assert.assertEquals(russianLegalEntity.getRussianBankAccount().getBankPostAccount(), contractorForUpdate.getRussianLegalEntityBankPostAccount());
        Assert.assertEquals(russianLegalEntity.getRegisteredName(), contractorForUpdate.getRussianLegalEntityName());
        Assert.assertEquals(russianLegalEntity.getPostAddress(), contractorForUpdate.getRussianLegalEntityPostAddress());
        Assert.assertEquals(russianLegalEntity.getRegisteredNumber(), contractorForUpdate.getRussianLegalEntityRegisteredNumber());
        Assert.assertEquals(russianLegalEntity.getRepresentativeDocument(), contractorForUpdate.getRussianLegalEntityRepresentativeDocument());
        Assert.assertEquals(russianLegalEntity.getRepresentativeFullName(), contractorForUpdate.getRussianLegalEntityRepresentativeFullName());
        Assert.assertEquals(russianLegalEntity.getRepresentativePosition(), contractorForUpdate.getRussianLegalEntityRepresentativePosition());
    }

    private void checkShopFields(RussianLegalEntity russianLegalEntity, String partyId, String shopId) {
        Shop contractorForUpdate = shopDao.getShopByPartyIdAndShopId(partyId, shopId);
        Assert.assertEquals(russianLegalEntity.getInn(), contractorForUpdate.getRussianLegalEntityInn());
        Assert.assertEquals(russianLegalEntity.getActualAddress(), contractorForUpdate.getRussianLegalEntityActualAddress());
        Assert.assertEquals(russianLegalEntity.getRussianBankAccount().getAccount(), contractorForUpdate.getRussianLegalEntityBankAccount());
        Assert.assertEquals(russianLegalEntity.getRussianBankAccount().getBankBik(), contractorForUpdate.getRussianLegalEntityBankBik());
        Assert.assertEquals(russianLegalEntity.getRussianBankAccount().getBankName(), contractorForUpdate.getRussianLegalEntityBankName());
        Assert.assertEquals(russianLegalEntity.getRussianBankAccount().getBankPostAccount(), contractorForUpdate.getRussianLegalEntityBankPostAccount());
        Assert.assertEquals(russianLegalEntity.getRegisteredName(), contractorForUpdate.getRussianLegalEntityName());
        Assert.assertEquals(russianLegalEntity.getPostAddress(), contractorForUpdate.getRussianLegalEntityPostAddress());
        Assert.assertEquals(russianLegalEntity.getRegisteredNumber(), contractorForUpdate.getRussianLegalEntityRegisteredNumber());
        Assert.assertEquals(russianLegalEntity.getRepresentativeDocument(), contractorForUpdate.getRussianLegalEntityRepresentativeDocument());
        Assert.assertEquals(russianLegalEntity.getRepresentativeFullName(), contractorForUpdate.getRussianLegalEntityRepresentativeFullName());
        Assert.assertEquals(russianLegalEntity.getRepresentativePosition(), contractorForUpdate.getRussianLegalEntityRepresentativePosition());
    }

}
