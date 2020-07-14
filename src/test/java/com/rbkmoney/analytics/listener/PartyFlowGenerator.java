package com.rbkmoney.analytics.listener;

import com.rbkmoney.damsel.domain.*;
import com.rbkmoney.damsel.payment_processing.*;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.geck.serializer.kit.mock.MockMode;
import com.rbkmoney.geck.serializer.kit.mock.MockTBaseProcessor;
import com.rbkmoney.geck.serializer.kit.tbase.TBaseHandler;
import com.rbkmoney.kafka.common.serialization.ThriftSerializer;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import com.rbkmoney.machinegun.eventsink.SinkEvent;
import com.rbkmoney.machinegun.msgpack.Value;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class PartyFlowGenerator {

    public static final String SOURCE_ID = "testPartyId";
    public static final String PARTY_ID = "testPartyId";
    public static final String SHOP_ID = "testShopId";
    public static final String PARTY_EMAIL = "testPartyEmail";
    public static final String SOURCE_NS = "source_ns";
    public static final String PARTY_BLOCK_REASON = "testBlockReason";
    public static final String SHOP_UNBLOCK_REASON = "testUnblockReason";
    public static final Long PARTY_REVISION_ID = 12345L;
    public static final Long CLAIM_ID = 524523L;
    public static final Integer REVISION_ID = 431531;
    public static final Integer CATEGORY_ID = 542432;
    public static final String CONTRACT_ID = "142534";
    public static final String PAYOUT_ID = "654635";
    public static final String DETAILS_NAME = "testDetailsName";
    public static final String DETAILS_DESCRIPTION = "testDescription";
    public static final Integer SCHEDULE_ID = 15643653;
    public static final String PAYOUT_TOOL_ID = "65373462";
    public static final String CURRENCY_SYMBOL = "RUB";
    public static final Long PAYOUT_ID_LONG = 654635L;
    public static final Long SETTLEMENT_ID = 245234L;
    public static final String CONTRACTOR_ID = "563462";

    public static List<SinkEvent> generatePartyFlow() throws IOException {
        List<SinkEvent> sinkEvents = new ArrayList<>();
        Long sequenceId = 0L;
        sinkEvents.add(buildSinkEvent(buildMessagePartyCreated(sequenceId++)));
        sinkEvents.add(buildSinkEvent(buildMessagePartyBlocking(sequenceId++)));
        sinkEvents.add(buildSinkEvent(buildMessagePartySuspension(sequenceId++)));
        sinkEvents.add(buildSinkEvent(buildMessagePartyRevisionChanged(sequenceId++)));
        sinkEvents.add(buildSinkEvent(buildContractorCreatedMapper(sequenceId++)));
        sinkEvents.add(buildSinkEvent(buildContractorIdentificationLevelChanged(sequenceId++)));
        sinkEvents.add(buildSinkEvent(buildMessageShopCreated(sequenceId++)));
        sinkEvents.add(buildSinkEvent(buildMessageShopBlocking(sequenceId++)));
        sinkEvents.add(buildSinkEvent(buildMessageShopSuspension(sequenceId++)));
        sinkEvents.add(buildSinkEvent(buildMessageShopCategoryChanged(sequenceId++)));
        sinkEvents.add(buildSinkEvent(buildMessageShopContractChanged(sequenceId++)));
        sinkEvents.add(buildSinkEvent(buildMessageShopDetailsChanged(sequenceId++)));
        sinkEvents.add(buildSinkEvent(buildMessageShopPayoutScheduleChanged(sequenceId++)));
        sinkEvents.add(buildSinkEvent(buildMessageShopPayoutToolChanged(sequenceId++)));
        sinkEvents.add(buildSinkEvent(buildMessageShopAccountCreated(sequenceId++)));

        return sinkEvents;
    }

    private static MachineEvent buildMessagePartyCreated(Long sequenceId) {
        PartyCreated partyCreated = buildPartyCreated();
        PartyChange partyChange = new PartyChange();
        partyChange.setPartyCreated(partyCreated);
        return buildMachineEvent(partyChange, SOURCE_ID, sequenceId);
    }

    private static MachineEvent buildMessagePartyBlocking(Long sequenceId) {
        Blocking blocking = buildPartyBlocking();
        PartyChange partyChange = new PartyChange();
        partyChange.setPartyBlocking(blocking);
        return buildMachineEvent(partyChange, SOURCE_ID, sequenceId);
    }

    private static MachineEvent buildMessagePartySuspension(Long sequenceId) {
        Suspension suspension = buildPartySuspension();
        PartyChange partyChange = new PartyChange();
        partyChange.setPartySuspension(suspension);
        return buildMachineEvent(partyChange, SOURCE_ID, sequenceId);
    }

    private static MachineEvent buildMessagePartyRevisionChanged(Long sequenceId) {
        PartyRevisionChanged partyRevisionChanged = buildPartyRevisionChanged();
        PartyChange partyChange = new PartyChange();
        partyChange.setRevisionChanged(partyRevisionChanged);
        return buildMachineEvent(partyChange, SOURCE_ID, sequenceId);
    }

    private static MachineEvent buildMessageShopBlocking(Long sequenceId) {
        ShopBlocking shopBlocking = buildShopBlocking();
        PartyChange partyChange = new PartyChange();
        partyChange.setShopBlocking(shopBlocking);
        return buildMachineEvent(partyChange, SOURCE_ID, sequenceId);
    }

    private static MachineEvent buildMessageShopSuspension(Long sequenceId) {
        ShopSuspension shopSuspension = buildShopSuspension();
        PartyChange partyChange = new PartyChange();
        partyChange.setShopSuspension(shopSuspension);
        return buildMachineEvent(partyChange, SOURCE_ID, sequenceId);
    }

    private static MachineEvent buildMessageShopCreated(Long sequenceId) throws IOException {
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
        return buildMachineEvent(partyChange, SOURCE_ID, sequenceId);
    }

    private static MachineEvent buildMessageShopCategoryChanged(Long sequenceId) {
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
        return buildMachineEvent(partyChange, SOURCE_ID, sequenceId);
    }

    private static MachineEvent buildMessageShopContractChanged(Long sequenceId) {
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
        return buildMachineEvent(partyChange, SOURCE_ID, sequenceId);
    }

    private static MachineEvent buildMessageShopDetailsChanged(Long sequenceId) {
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
        return buildMachineEvent(partyChange, SOURCE_ID, sequenceId);
    }

    private static MachineEvent buildMessageShopPayoutScheduleChanged(Long sequenceId) {
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
        return buildMachineEvent(partyChange, SOURCE_ID, sequenceId);
    }

    private static MachineEvent buildMessageShopPayoutToolChanged(Long sequenceId) {
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
        return buildMachineEvent(partyChange, SOURCE_ID, sequenceId);
    }

    private static MachineEvent buildMessageShopAccountCreated(Long sequenceId) {
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
        return buildMachineEvent(partyChange, SOURCE_ID, sequenceId);
    }

    private static MachineEvent buildContractorCreatedMapper(Long sequenceId) throws IOException {
        ContractorEffectUnit contractorEffectUnit = new ContractorEffectUnit();
        contractorEffectUnit.setId(CONTRACTOR_ID);
        ContractorEffect contractorEffect = new ContractorEffect();
        contractorEffect.setCreated(buildPartyContractor());
        contractorEffectUnit.setEffect(contractorEffect);
        ClaimEffect claimEffect = new ClaimEffect();
        claimEffect.setContractorEffect(contractorEffectUnit);
        Claim claim = buildClaimCreated(claimEffect);
        PartyChange partyChange = new PartyChange();
        partyChange.setClaimCreated(claim);
        return buildMachineEvent(partyChange, SOURCE_ID, sequenceId);
    }

    private static MachineEvent buildContractorIdentificationLevelChanged(Long sequenceId) {
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
        return buildMachineEvent(partyChange, SOURCE_ID, sequenceId);
    }

    private static PartyContractor buildPartyContractor() throws IOException {
        PartyContractor partyContractor = new PartyContractor();
        partyContractor.setId(PARTY_ID);
        partyContractor.setStatus(ContractorIdentificationLevel.full);
        Contractor contractor = new Contractor();
        contractor = new MockTBaseProcessor(MockMode.ALL).process(contractor, new TBaseHandler<>(Contractor.class));
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
        blocking.setBlocked(new Blocked(PARTY_BLOCK_REASON, TypeUtil.temporalToString(LocalDateTime.now())));
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

    private static PartyCreated buildPartyCreated() {
        return new PartyCreated(PARTY_ID, new PartyContactInfo(PARTY_EMAIL), TypeUtil.temporalToString(LocalDateTime.now()));
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
        EventPayload payload = new EventPayload();
        ArrayList<PartyChange> partyChanges = new ArrayList<>();
        partyChanges.add(partyChange);
        payload.setPartyChanges(partyChanges);

        message.setCreatedAt(TypeUtil.temporalToString(Instant.now()));
        message.setEventId(sequenceId);
        message.setSourceNs(SOURCE_NS);
        message.setSourceId(sourceId);

        ThriftSerializer<EventPayload> eventPayloadThriftSerializer = new ThriftSerializer<>();
        Value data = new Value();
        data.setBin(eventPayloadThriftSerializer.serialize("", payload));
        message.setData(data);
        return message;
    }

    private static SinkEvent buildSinkEvent(MachineEvent machineEvent) {
        SinkEvent sinkEvent = new SinkEvent();
        sinkEvent.setEvent(machineEvent);
        return sinkEvent;
    }

}
