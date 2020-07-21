package com.rbkmoney.analytics.listener;

import com.rbkmoney.damsel.domain.*;
import com.rbkmoney.damsel.payment_processing.*;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.geck.serializer.kit.mock.MockMode;
import com.rbkmoney.geck.serializer.kit.mock.MockTBaseProcessor;
import com.rbkmoney.geck.serializer.kit.tbase.TBaseHandler;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import com.rbkmoney.machinegun.eventsink.SinkEvent;
import com.rbkmoney.machinegun.msgpack.Value;
import com.rbkmoney.sink.common.serialization.impl.PartyEventDataSerializer;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.*;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class PartyFlowGenerator {

    public static final String SHOP_ID = "testShopId";
    public static final String PARTY_EMAIL = "testPartyEmail";
    public static final String SOURCE_NS = "source_ns";
    public static final String PARTY_BLOCK_REASON = "testPartyBlockReason";
    public static final String SHOP_BLOCK_REASON = "testShopBlockReason";
    public static final String SHOP_UNBLOCK_REASON = "testShopUnblockReason";
    public static final Long PARTY_REVISION_ID = 12345L;
    public static final Long CLAIM_ID = 524523L;
    public static final Integer REVISION_ID = 431531;
    public static final Integer CATEGORY_ID = 542432;
    public static final String CONTRACT_ID = "142534";
    public static final String PAYOUT_ID = "654635";
    public static final String DETAILS_NAME = "testDetailsName";
    public static final String DETAILS_DESCRIPTION = "testDescription";
    public static final Integer SCHEDULE_ID = 15643653;
    public static final String PAYOUT_TOOL_ID = "654635";
    public static final String CURRENCY_SYMBOL = "RUB";
    public static final Long PAYOUT_ID_LONG = 654635L;
    public static final Long SETTLEMENT_ID = 245234L;
    public static final String CONTRACTOR_ID = "563462";

    public static List<SinkEvent> generatePartyFlow(String partyId) throws IOException {
        List<SinkEvent> sinkEvents = new ArrayList<>();
        Long sequenceId = 0L;
        sinkEvents.add(buildSinkEvent(buildMessagePartyCreated(sequenceId++, partyId)));
        sinkEvents.add(buildSinkEvent(buildMessagePartyBlocking(sequenceId++, partyId)));
        sinkEvents.add(buildSinkEvent(buildMessagePartySuspension(sequenceId++, partyId)));
        sinkEvents.add(buildSinkEvent(buildMessagePartyRevisionChanged(sequenceId++, partyId)));
        sinkEvents.add(buildSinkEvent(buildContractorCreatedMapper(sequenceId++, buildPartyContractor(partyId), partyId)));
        sinkEvents.add(buildSinkEvent(buildContractorIdentificationLevelChanged(sequenceId++, partyId)));
        sinkEvents.add(buildSinkEvent(buildMessageShopCreated(sequenceId++, partyId)));
        sinkEvents.add(buildSinkEvent(buildMessageShopBlocking(sequenceId++, partyId)));
        sinkEvents.add(buildSinkEvent(buildMessageShopSuspension(sequenceId++, partyId)));
        sinkEvents.add(buildSinkEvent(buildMessageShopCategoryChanged(sequenceId++, partyId)));
        sinkEvents.add(buildSinkEvent(buildMessageShopContractChanged(sequenceId++, partyId)));
        sinkEvents.add(buildSinkEvent(buildMessageShopDetailsChanged(sequenceId++, partyId)));
        sinkEvents.add(buildSinkEvent(buildMessageShopPayoutScheduleChanged(sequenceId++, partyId)));
        sinkEvents.add(buildSinkEvent(buildMessageShopPayoutToolChanged(sequenceId++, partyId)));
        sinkEvents.add(buildSinkEvent(buildMessageShopAccountCreated(sequenceId++, partyId)));

        return sinkEvents;
    }

    public static List<SinkEvent> generatePartyContractorFlow(String partyId) throws IOException {
        List<SinkEvent> sinkEvents = new ArrayList<>();
        Long sequenceId = 0L;
        sinkEvents.add(buildSinkEvent(buildMessagePartyCreated(sequenceId++, partyId)));
        sinkEvents.add(buildSinkEvent(buildMessagePartyBlocking(sequenceId++, partyId)));
        sinkEvents.add(buildSinkEvent(buildMessagePartySuspension(sequenceId++, partyId)));
        sinkEvents.add(buildSinkEvent(buildMessagePartyRevisionChanged(sequenceId++, partyId)));
        sinkEvents.add(buildSinkEvent(buildContractorCreatedMapper(sequenceId++, buildLegalPartyContractor(partyId), partyId)));
        sinkEvents.add(buildSinkEvent(buildContractorIdentificationLevelChanged(sequenceId++, partyId)));

        return sinkEvents;
    }

    public static List<SinkEvent> generateShopFlow(String partyId) throws IOException {
        List<SinkEvent> sinkEvents = new ArrayList<>();
        Long sequenceId = 0L;
        sinkEvents.add(buildSinkEvent(buildMessagePartyCreated(sequenceId++, partyId)));
        sinkEvents.add(buildSinkEvent(buildMessageShopCreated(sequenceId++, partyId)));
        sinkEvents.add(buildSinkEvent(buildMessageShopBlocking(sequenceId++, partyId)));
        sinkEvents.add(buildSinkEvent(buildMessageShopSuspension(sequenceId++, partyId)));
        sinkEvents.add(buildSinkEvent(buildMessageShopCategoryChanged(sequenceId++, partyId)));
        sinkEvents.add(buildSinkEvent(buildMessageShopContractChanged(sequenceId++, partyId)));
        sinkEvents.add(buildSinkEvent(buildMessageShopDetailsChanged(sequenceId++, partyId)));
        sinkEvents.add(buildSinkEvent(buildMessageShopPayoutScheduleChanged(sequenceId++, partyId)));
        sinkEvents.add(buildSinkEvent(buildMessageShopPayoutToolChanged(sequenceId++, partyId)));
        sinkEvents.add(buildSinkEvent(buildMessageShopAccountCreated(sequenceId++, partyId)));

        return sinkEvents;
    }

    private static MachineEvent buildMessagePartyCreated(Long sequenceId, String partyId) {
        PartyCreated partyCreated = buildPartyCreated(partyId);
        PartyChange partyChange = new PartyChange();
        partyChange.setPartyCreated(partyCreated);
        return buildMachineEvent(partyChange, partyId, sequenceId);
    }

    private static MachineEvent buildMessagePartyBlocking(Long sequenceId, String partyId) {
        Blocking blocking = buildPartyBlocking();
        PartyChange partyChange = new PartyChange();
        partyChange.setPartyBlocking(blocking);
        return buildMachineEvent(partyChange, partyId, sequenceId);
    }

    private static MachineEvent buildMessagePartySuspension(Long sequenceId, String partyId) {
        Suspension suspension = buildPartySuspension();
        PartyChange partyChange = new PartyChange();
        partyChange.setPartySuspension(suspension);
        return buildMachineEvent(partyChange, partyId, sequenceId);
    }

    private static MachineEvent buildMessagePartyRevisionChanged(Long sequenceId, String partyId) {
        PartyRevisionChanged partyRevisionChanged = buildPartyRevisionChanged();
        PartyChange partyChange = new PartyChange();
        partyChange.setRevisionChanged(partyRevisionChanged);
        return buildMachineEvent(partyChange, partyId, sequenceId);
    }

    private static MachineEvent buildMessageShopBlocking(Long sequenceId, String partyId) {
        ShopBlocking shopBlocking = buildShopBlocking();
        PartyChange partyChange = new PartyChange();
        partyChange.setShopBlocking(shopBlocking);
        return buildMachineEvent(partyChange, partyId, sequenceId);
    }

    private static MachineEvent buildMessageShopSuspension(Long sequenceId, String partyId) {
        ShopSuspension shopSuspension = buildShopSuspension();
        PartyChange partyChange = new PartyChange();
        partyChange.setShopSuspension(shopSuspension);
        return buildMachineEvent(partyChange, partyId, sequenceId);
    }

    private static MachineEvent buildMessageShopCreated(Long sequenceId, String partyId) throws IOException {
        Shop shop = buildShopCreated();
        ShopEffectUnit shopEffectUnit = new ShopEffectUnit();
        shopEffectUnit.setShopId(SHOP_ID);
        ShopEffect shopEffect = new ShopEffect();
        shopEffect.setCreated(buildShopCreated());
        shopEffectUnit.setEffect(shopEffect);
        ClaimEffect claimEffect = new ClaimEffect();
        claimEffect.setShopEffect(shopEffectUnit);
        Claim claim = buildClaimCreated(claimEffect);
        PartyChange partyChange = new PartyChange();
        partyChange.setClaimCreated(claim);
        return buildMachineEvent(partyChange, partyId, sequenceId);
    }

    private static MachineEvent buildMessageShopCategoryChanged(Long sequenceId, String partyId) {
        ShopEffectUnit shopEffectUnit = new ShopEffectUnit();
        shopEffectUnit.setShopId(SHOP_ID);
        ShopEffect shopEffect = new ShopEffect();
        CategoryRef categoryRef = new CategoryRef();
        categoryRef.setId(CATEGORY_ID);
        shopEffect.setCategoryChanged(categoryRef);
        shopEffectUnit.setEffect(shopEffect);
        ClaimEffect claimEffect = new ClaimEffect();
        claimEffect.setShopEffect(shopEffectUnit);
        Claim claim = buildClaimCreated(claimEffect);
        PartyChange partyChange = new PartyChange();
        partyChange.setClaimCreated(claim);
        return buildMachineEvent(partyChange, partyId, sequenceId);
    }

    private static MachineEvent buildMessageShopContractChanged(Long sequenceId, String partyId) {
        ShopEffectUnit shopEffectUnit = new ShopEffectUnit();
        shopEffectUnit.setShopId(SHOP_ID);
        ShopContractChanged shopContractChanged = new ShopContractChanged();
        shopContractChanged.setContractId(CONTRACT_ID);
        shopContractChanged.setPayoutToolId(PAYOUT_ID);
        ShopEffect shopEffect = new ShopEffect();
        shopEffect.setContractChanged(shopContractChanged);
        shopEffectUnit.setEffect(shopEffect);
        ClaimEffect claimEffect = new ClaimEffect();
        claimEffect.setShopEffect(shopEffectUnit);
        Claim claim = buildClaimCreated(claimEffect);
        PartyChange partyChange = new PartyChange();
        partyChange.setClaimCreated(claim);
        return buildMachineEvent(partyChange, partyId, sequenceId);
    }

    private static MachineEvent buildMessageShopDetailsChanged(Long sequenceId, String partyId) {
        ShopEffectUnit shopEffectUnit = new ShopEffectUnit();
        shopEffectUnit.setShopId(SHOP_ID);
        ShopDetails shopDetails = new ShopDetails();
        shopDetails.setName(DETAILS_NAME);
        shopDetails.setDescription(DETAILS_DESCRIPTION);
        ShopEffect shopEffect = new ShopEffect();
        shopEffect.setDetailsChanged(shopDetails);
        shopEffectUnit.setEffect(shopEffect);
        ClaimEffect claimEffect = new ClaimEffect();
        claimEffect.setShopEffect(shopEffectUnit);
        Claim claim = buildClaimCreated(claimEffect);
        PartyChange partyChange = new PartyChange();
        partyChange.setClaimCreated(claim);
        return buildMachineEvent(partyChange, partyId, sequenceId);
    }

    private static MachineEvent buildMessageShopPayoutScheduleChanged(Long sequenceId, String partyId) {
        ShopEffectUnit shopEffectUnit = new ShopEffectUnit();
        shopEffectUnit.setShopId(SHOP_ID);
        ScheduleChanged scheduleChanged = new ScheduleChanged();
        scheduleChanged.setSchedule(new BusinessScheduleRef(SCHEDULE_ID));
        ShopEffect shopEffect = new ShopEffect();
        shopEffect.setPayoutScheduleChanged(scheduleChanged);
        shopEffectUnit.setEffect(shopEffect);
        ClaimEffect claimEffect = new ClaimEffect();
        claimEffect.setShopEffect(shopEffectUnit);
        Claim claim = buildClaimCreated(claimEffect);
        PartyChange partyChange = new PartyChange();
        partyChange.setClaimCreated(claim);
        return buildMachineEvent(partyChange, partyId, sequenceId);
    }

    private static MachineEvent buildMessageShopPayoutToolChanged(Long sequenceId, String partyId) {
        ShopEffectUnit shopEffectUnit = new ShopEffectUnit();
        shopEffectUnit.setShopId(SHOP_ID);
        ShopEffect shopEffect = new ShopEffect();
        shopEffect.setPayoutToolChanged(PAYOUT_TOOL_ID);
        shopEffectUnit.setEffect(shopEffect);
        ClaimEffect claimEffect = new ClaimEffect();
        claimEffect.setShopEffect(shopEffectUnit);
        Claim claim = buildClaimCreated(claimEffect);
        PartyChange partyChange = new PartyChange();
        partyChange.setClaimCreated(claim);
        return buildMachineEvent(partyChange, partyId, sequenceId);
    }

    private static MachineEvent buildMessageShopAccountCreated(Long sequenceId, String partyId) {
        ShopEffectUnit shopEffectUnit = new ShopEffectUnit();
        shopEffectUnit.setShopId(SHOP_ID);
        ShopAccount shopAccount = new ShopAccount();
        shopAccount.setCurrency(new CurrencyRef(CURRENCY_SYMBOL));
        shopAccount.setPayout(PAYOUT_ID_LONG);
        shopAccount.setSettlement(SETTLEMENT_ID);
        ShopEffect shopEffect = new ShopEffect();
        shopEffect.setAccountCreated(shopAccount);
        shopEffectUnit.setEffect(shopEffect);
        ClaimEffect claimEffect = new ClaimEffect();
        claimEffect.setShopEffect(shopEffectUnit);
        Claim claim = buildClaimCreated(claimEffect);
        PartyChange partyChange = new PartyChange();
        partyChange.setClaimCreated(claim);
        return buildMachineEvent(partyChange, partyId, sequenceId);
    }

    private static MachineEvent buildContractorCreatedMapper(Long sequenceId, PartyContractor partyContractor, String partyId) throws IOException {
        ContractorEffectUnit contractorEffectUnit = new ContractorEffectUnit();
        contractorEffectUnit.setId(CONTRACTOR_ID);
        ContractorEffect contractorEffect = new ContractorEffect();
        contractorEffect.setCreated(partyContractor);
        contractorEffectUnit.setEffect(contractorEffect);
        ClaimEffect claimEffect = new ClaimEffect();
        claimEffect.setContractorEffect(contractorEffectUnit);
        Claim claim = buildClaimCreated(claimEffect);
        PartyChange partyChange = new PartyChange();
        partyChange.setClaimCreated(claim);
        return buildMachineEvent(partyChange, partyId, sequenceId);
    }

    private static MachineEvent buildContractorIdentificationLevelChanged(Long sequenceId, String partyId) {
        ContractorEffectUnit contractorEffectUnit = new ContractorEffectUnit();
        contractorEffectUnit.setId(CONTRACTOR_ID);
        ContractorEffect contractorEffect = new ContractorEffect();
        contractorEffect.setIdentificationLevelChanged(ContractorIdentificationLevel.partial);
        contractorEffectUnit.setEffect(contractorEffect);
        ClaimEffect claimEffect = new ClaimEffect();
        claimEffect.setContractorEffect(contractorEffectUnit);
        Claim claim = buildClaimCreated(claimEffect);
        PartyChange partyChange = new PartyChange();
        partyChange.setClaimCreated(claim);
        return buildMachineEvent(partyChange, partyId, sequenceId);
    }

    private static PartyContractor buildPartyContractor(String partyId) throws IOException {
        PartyContractor partyContractor = new PartyContractor();
        partyContractor.setId(partyId);
        partyContractor.setStatus(ContractorIdentificationLevel.full);
        Contractor contractor = new Contractor();
        contractor = new MockTBaseProcessor(MockMode.ALL).process(contractor, new TBaseHandler<>(Contractor.class));
        partyContractor.setContractor(contractor);
        partyContractor.setIdentityDocuments(Collections.emptyList());
        return partyContractor;
    }

    private static PartyContractor buildLegalPartyContractor(String partyId) throws IOException {
        PartyContractor partyContractor = new PartyContractor();
        partyContractor.setId(partyId);
        partyContractor.setStatus(ContractorIdentificationLevel.none);
        Contractor contractor = new Contractor();
        LegalEntity legalEntity = new LegalEntity();
        RussianLegalEntity russianLegalEntity = new RussianLegalEntity();
        russianLegalEntity = new MockTBaseProcessor(MockMode.ALL).process(russianLegalEntity, new TBaseHandler<>(RussianLegalEntity.class));
        legalEntity.setRussianLegalEntity(russianLegalEntity);
        contractor.setLegalEntity(legalEntity);
        partyContractor.setContractor(contractor);
        partyContractor.setIdentityDocuments(Collections.emptyList());
        return partyContractor;
    }

    private static Claim buildClaimCreated(ClaimEffect claimEffect) {
        ClaimAccepted claimAccepted = new ClaimAccepted();
        claimAccepted.setEffects(Collections.singletonList(claimEffect));
        ClaimStatus claimStatus = ClaimStatus.accepted(claimAccepted);
        return new Claim(CLAIM_ID, claimStatus, Collections.emptyList(), REVISION_ID, TypeUtil.temporalToString(LocalDateTime.now()));
    }

    private static Shop buildShopCreated() throws IOException {
        Shop shop = new Shop();
        shop = new MockTBaseProcessor(MockMode.ALL).process(shop, new TBaseHandler<>(Shop.class));
        shop.setCreatedAt(TypeUtil.temporalToString(LocalDateTime.now()));
        Blocking blocking = new Blocking();
        blocking.setBlocked(new Blocked(SHOP_BLOCK_REASON, TypeUtil.temporalToString(LocalDateTime.now())));
        shop.setBlocking(blocking);
        shop.setSuspension(buildPartySuspension());
        return shop;
    }

    private static ShopSuspension buildShopSuspension() {
        Suspension suspension = new Suspension();
        suspension.setActive(new Active(TypeUtil.temporalToString(LocalDateTime.now())));
        return new ShopSuspension(SHOP_ID, suspension);
    }

    private static ShopBlocking buildShopBlocking() {
        Blocking blocking = new Blocking();
        blocking.setUnblocked(new Unblocked(SHOP_UNBLOCK_REASON, TypeUtil.temporalToString(LocalDateTime.now())));
        return new ShopBlocking(SHOP_ID, blocking);
    }

    private static PartyRevisionChanged buildPartyRevisionChanged() {
        return new PartyRevisionChanged(TypeUtil.temporalToString(LocalDateTime.now()), PARTY_REVISION_ID);
    }

    private static PartyCreated buildPartyCreated(String partyId) {
        return new PartyCreated(partyId, new PartyContactInfo(PARTY_EMAIL), TypeUtil.temporalToString(LocalDateTime.now()));
    }

    private static Suspension buildPartySuspension() {
        Suspension suspension = new Suspension();
        suspension.setActive(new Active(TypeUtil.temporalToString(LocalDateTime.now())));

        return suspension;
    }

    private static Blocking buildPartyBlocking() {
        Blocking blocking = new Blocking();
        blocking.setBlocked(new Blocked(PARTY_BLOCK_REASON, TypeUtil.temporalToString(LocalDateTime.now())));
        return blocking;
    }

    private static MachineEvent buildMachineEvent(PartyChange partyChange, String sourceId, Long sequenceId) {
        MachineEvent message = new MachineEvent();
        ArrayList<PartyChange> partyChanges = new ArrayList<>();
        partyChanges.add(partyChange);

        message.setCreatedAt(TypeUtil.temporalToString(Instant.now()));
        message.setEventId(sequenceId);
        message.setSourceNs(SOURCE_NS);
        message.setSourceId(sourceId);

        PartyEventDataSerializer partyEventDataSerializer = new PartyEventDataSerializer();
        Value data = new Value();
        data.setBin(partyEventDataSerializer.serialize(new PartyEventData(partyChanges)));
        message.setData(data);
        return message;
    }

    private static SinkEvent buildSinkEvent(MachineEvent machineEvent) {
        SinkEvent sinkEvent = new SinkEvent();
        sinkEvent.setEvent(machineEvent);
        return sinkEvent;
    }

}
