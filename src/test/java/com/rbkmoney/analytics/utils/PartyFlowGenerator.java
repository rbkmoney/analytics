package com.rbkmoney.analytics.utils;

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
    public static final Long SETTLEMENT_ID = 245234L;
    public static final String CONTRACTOR_ID = "563462";
    public static final Long SHOP_ACCOUNT_PAYOUT = 5425234L;
    public static final String INN = "213123123123";

    public static List<SinkEvent> generatePartyFlow(String partyId, String shopId) throws IOException {
        List<SinkEvent> sinkEvents = new ArrayList<>();
        Long sequenceId = 0L;
        sinkEvents.add(buildSinkEvent(buildMessagePartyCreated(sequenceId++, partyId)));
        sinkEvents.add(buildSinkEvent(buildMessagePartyBlocking(sequenceId++, partyId)));
        sinkEvents.add(buildSinkEvent(buildMessagePartySuspension(sequenceId++, partyId)));
        sinkEvents.add(buildSinkEvent(buildMessagePartyRevisionChanged(sequenceId++, partyId)));
        sinkEvents.add(buildSinkEvent(buildContractCreated(sequenceId++, buildContract(CONTRACT_ID), partyId)));
        sinkEvents.add(buildSinkEvent(buildContractorCreated(sequenceId++, buildPartyContractor(), partyId)));
        sinkEvents.add(buildSinkEvent(buildContractorIdentificationLevelChanged(sequenceId++, partyId, CONTRACTOR_ID)));
        sinkEvents.add(buildSinkEvent(buildMessageShopCreated(sequenceId++, partyId, shopId, CONTRACT_ID)));
        sinkEvents.add(buildSinkEvent(buildMessageShopBlocking(sequenceId++, partyId, shopId)));
        sinkEvents.add(buildSinkEvent(buildMessageShopSuspension(sequenceId++, partyId, shopId)));
        sinkEvents.add(buildSinkEvent(buildMessageShopCategoryChanged(sequenceId++, partyId, shopId)));
        sinkEvents.add(buildSinkEvent(buildMessageShopContractChanged(sequenceId++, partyId, shopId)));
        sinkEvents.add(buildSinkEvent(buildMessageShopDetailsChanged(sequenceId++, partyId, shopId)));
        sinkEvents.add(buildSinkEvent(buildMessageShopPayoutScheduleChanged(sequenceId++, partyId, shopId)));
        sinkEvents.add(buildSinkEvent(buildMessageShopPayoutToolChanged(sequenceId++, partyId, shopId)));
        sinkEvents.add(buildSinkEvent(buildMessageShopAccountCreated(sequenceId++, partyId, shopId)));

        return sinkEvents;
    }

    public static List<SinkEvent> generatePartyContractorFlow(String partyId) throws IOException {
        List<SinkEvent> sinkEvents = new ArrayList<>();
        Long sequenceId = 0L;
        sinkEvents.add(buildSinkEvent(buildMessagePartyCreated(sequenceId++, partyId)));
        sinkEvents.add(buildSinkEvent(buildMessagePartyBlocking(sequenceId++, partyId)));
        sinkEvents.add(buildSinkEvent(buildMessagePartySuspension(sequenceId++, partyId)));
        sinkEvents.add(buildSinkEvent(buildMessagePartyRevisionChanged(sequenceId++, partyId)));
        sinkEvents.add(buildSinkEvent(buildContractorCreated(sequenceId++, buildRussianLegalPartyContractor(), partyId)));
        sinkEvents.add(buildSinkEvent(buildContractorIdentificationLevelChanged(sequenceId, partyId, CONTRACTOR_ID)));
        return sinkEvents;
    }

    public static List<SinkEvent> generateShopFlow(String partyId, String shopId) throws IOException {
        List<SinkEvent> sinkEvents = new ArrayList<>();
        Long sequenceId = 0L;
        sinkEvents.add(buildSinkEvent(buildMessagePartyCreated(sequenceId++, partyId)));
        final Contract contract = buildContract(CONTRACT_ID);
        sinkEvents.add(buildSinkEvent(buildContractCreated(sequenceId++, contract, partyId)));
        sinkEvents.add(buildSinkEvent(buildMessageShopCreated(sequenceId++, partyId, shopId, CONTRACT_ID)));
        sinkEvents.add(buildSinkEvent(buildContractorCreated(sequenceId++, buildRussianLegalPartyContractor(), partyId)));
        sinkEvents.add(buildSinkEvent(buildContractorIdChange(sequenceId++, contract, partyId, CONTRACTOR_ID)));
        sinkEvents.add(buildSinkEvent(buildMessageShopBlocking(sequenceId++, partyId, shopId)));
        sinkEvents.add(buildSinkEvent(buildMessageShopPayoutToolChanged(sequenceId++, partyId, shopId)));
        sinkEvents.add(buildSinkEvent(buildMessageShopSuspension(sequenceId++, partyId, shopId)));
        sinkEvents.add(buildSinkEvent(buildMessageShopCategoryChanged(sequenceId++, partyId, shopId)));
        sinkEvents.add(buildSinkEvent(buildMessageShopContractChanged(sequenceId++, partyId, shopId)));
        sinkEvents.add(buildSinkEvent(buildMessageShopDetailsChanged(sequenceId++, partyId, shopId)));
        sinkEvents.add(buildSinkEvent(buildMessageShopPayoutScheduleChanged(sequenceId++, partyId, shopId)));
        sinkEvents.add(buildSinkEvent(buildMessageShopAccountCreated(sequenceId++, partyId, shopId)));

        sinkEvents.add(buildSinkEvent(buildMessageShopSuspension(
                sequenceId++, partyId, buildSuspendedShopSuspension(TypeUtil.temporalToString(LocalDateTime.now()), shopId))));

        return sinkEvents;
    }

    public static List<SinkEvent> generatePartyFlowWithCount(int count, String lastPartyId, String lastShopId,
                                                             PartyContractor contractor) throws IOException {
        List<SinkEvent> sinkEvents = new ArrayList<>();
        Long sequenceId = 0L;
        for (int i = 0; i < count; i++) {
            final String contractId = String.valueOf(i);
            if (i == count - 1) {
                sinkEvents.add(buildSinkEvent(buildMessagePartyCreated(sequenceId++, lastPartyId)));
                sinkEvents.add(buildSinkEvent(buildMessagePartyBlocking(sequenceId++, lastPartyId)));
                final Contract contract = buildContract(contractId);
                sinkEvents.add(buildSinkEvent(buildContractCreated(sequenceId++, contract, lastPartyId)));
                sinkEvents.add(buildSinkEvent(buildContractorCreated(sequenceId++, contractor, lastPartyId)));
                sinkEvents.add(buildSinkEvent(buildContractorIdChange(sequenceId++, contract, lastPartyId, CONTRACTOR_ID)));
                sinkEvents.add(buildSinkEvent(buildContractorIdentificationLevelChanged(sequenceId++, lastPartyId, contractId)));
                sinkEvents.add(buildSinkEvent(buildMessageShopCreated(sequenceId++, lastPartyId, lastShopId, contractId)));
            } else {
                String partyId = UUID.randomUUID().toString();
                sinkEvents.add(buildSinkEvent(buildMessagePartyCreated(sequenceId++, partyId)));
                final Contract contract = buildContract(contractId);
                sinkEvents.add(buildSinkEvent(buildContractCreated(sequenceId++, contract, partyId)));
                final PartyContractor partyContractor = buildRussianLegalPartyContractor();
                sinkEvents.add(buildSinkEvent(buildContractorCreated(sequenceId++, partyContractor, partyId)));
                sinkEvents.add(buildSinkEvent(buildContractorIdChange(sequenceId++, contract, partyId, CONTRACTOR_ID)));
                sinkEvents.add(buildSinkEvent(buildContractorIdentificationLevelChanged(sequenceId++, partyId, contractId)));
                sinkEvents.add(buildSinkEvent(buildMessageShopCreated(sequenceId++, partyId, lastShopId, contractId)));
                sinkEvents.add(buildSinkEvent(buildMessagePartyBlocking(sequenceId++, partyId)));
                sinkEvents.add(buildSinkEvent(buildMessagePartySuspension(sequenceId++, partyId)));
                sinkEvents.add(buildSinkEvent(buildMessagePartyRevisionChanged(sequenceId++, partyId)));
            }
        }

        return sinkEvents;
    }

    public static List<SinkEvent> generatePartyFlowWithMultiplePartyChange(int count,
                                                                           String lastPartyId,
                                                                           PartyContractor partyContractor) throws IOException {
        List<SinkEvent> sinkEvents = new ArrayList<>();
        Long sequenceId = 0L;
        for (int i = 0; i < count; i++) {
            if (i == count - 1) {
                PartyChange partyChange = buildContractorCreatedPartyChange(partyContractor);
                sinkEvents.add(buildSinkEvent(buildMultiPartyChange(sequenceId, lastPartyId, partyChange)));
            } else {
                String partyId = UUID.randomUUID().toString();
                sinkEvents.add(buildSinkEvent(buildMultiPartyChange(sequenceId, partyId, null)));
            }
        }

        return sinkEvents;
    }

    public static MachineEvent buildMessagePartyCreated(Long sequenceId, String partyId) {
        PartyChange partyChange = buildPartyCreatedPartyChange(partyId);
        return buildMachineEvent(partyId, sequenceId, partyChange);
    }

    public static PartyChange buildPartyCreatedPartyChange(String partyId) {
        PartyCreated partyCreated = buildPartyCreated(partyId);
        PartyChange partyChange = new PartyChange();
        partyChange.setPartyCreated(partyCreated);
        return partyChange;
    }

    public static MachineEvent buildMessagePartyBlocking(Long sequenceId, String partyId) {
        PartyChange partyChange = buildPartyBlockingPartyChange();
        return buildMachineEvent(partyId, sequenceId, partyChange);
    }

    public static PartyChange buildPartyBlockingPartyChange() {
        Blocking blocking = buildPartyBlocking();
        PartyChange partyChange = new PartyChange();
        partyChange.setPartyBlocking(blocking);
        return partyChange;
    }

    public static MachineEvent buildMessagePartySuspension(Long sequenceId, String partyId) {
        PartyChange partyChange = buildPartySuspensionPartyChange();
        return buildMachineEvent(partyId, sequenceId, partyChange);
    }

    public static PartyChange buildPartySuspensionPartyChange() {
        Suspension suspension = buildPartySuspension();
        PartyChange partyChange = new PartyChange();
        partyChange.setPartySuspension(suspension);
        return partyChange;
    }

    public static MachineEvent buildMessagePartyRevisionChanged(Long sequenceId, String partyId) {
        PartyChange partyChange = buildPartyRevisionChangedPartyChange();
        return buildMachineEvent(partyId, sequenceId, partyChange);
    }

    public static PartyChange buildPartyRevisionChangedPartyChange() {
        PartyRevisionChanged partyRevisionChanged = buildPartyRevisionChanged();
        PartyChange partyChange = new PartyChange();
        partyChange.setRevisionChanged(partyRevisionChanged);
        return partyChange;
    }

    public static MachineEvent buildMessageShopBlocking(Long sequenceId, String partyId, String shopId) {
        PartyChange partyChange = buildShopBlockingPartyChange(shopId);
        return buildMachineEvent(partyId, sequenceId, partyChange);
    }

    public static PartyChange buildShopBlockingPartyChange(String shopId) {
        ShopBlocking shopBlocking = buildShopBlocking(shopId);
        PartyChange partyChange = new PartyChange();
        partyChange.setShopBlocking(shopBlocking);
        return partyChange;
    }

    public static MachineEvent buildMessageShopSuspension(Long sequenceId, String partyId, String shopId) {
        PartyChange partyChange = buildShopSuspensionPartyChange(shopId);
        return buildMachineEvent(partyId, sequenceId, partyChange);
    }

    public static PartyChange buildShopSuspensionPartyChange(String shopId) {
        ShopSuspension shopSuspension = buildActiveShopSuspension(TypeUtil.temporalToString(LocalDateTime.now()), shopId);
        PartyChange partyChange = new PartyChange();
        partyChange.setShopSuspension(shopSuspension);
        return partyChange;
    }

    public static MachineEvent buildMessageShopSuspension(Long sequenceId, String partyId, ShopSuspension shopSuspension) {
        PartyChange partyChange = new PartyChange();
        partyChange.setShopSuspension(shopSuspension);
        return buildMachineEvent(partyId, sequenceId, partyChange);
    }

    public static MachineEvent buildMessageShopCreated(Long sequenceId, String partyId, String shopId, String contractId) throws IOException {
        PartyChange partyChange = buildShopCreatedPartyChange(shopId, contractId);
        return buildMachineEvent(partyId, sequenceId, partyChange);
    }

    public static PartyChange buildShopCreatedPartyChange(String shopId, String contractId) throws IOException {
        ShopEffectUnit shopEffectUnit = new ShopEffectUnit();
        shopEffectUnit.setShopId(shopId);
        ShopEffect shopEffect = new ShopEffect();
        shopEffect.setCreated(buildShopCreated(contractId));
        shopEffectUnit.setEffect(shopEffect);
        ClaimEffect claimEffect = new ClaimEffect();
        claimEffect.setShopEffect(shopEffectUnit);
        Claim claim = buildClaimCreated(claimEffect);
        PartyChange partyChange = new PartyChange();
        partyChange.setClaimCreated(claim);

        return partyChange;
    }

    public static MachineEvent buildMessageShopCategoryChanged(Long sequenceId, String partyId, String shopId) {
        PartyChange partyChange = buildShopCategoryPartyChange(shopId);
        return buildMachineEvent(partyId, sequenceId, partyChange);
    }

    public static PartyChange buildShopCategoryPartyChange(String shopId) {
        ShopEffectUnit shopEffectUnit = new ShopEffectUnit();
        shopEffectUnit.setShopId(shopId);
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
        return partyChange;
    }

    public static MachineEvent buildMessageShopContractChanged(Long sequenceId, String partyId, String shopId) {
        PartyChange partyChange = buildShopContractPartyChange(shopId);
        return buildMachineEvent(partyId, sequenceId, partyChange);
    }

    public static PartyChange buildShopContractPartyChange(String shopId) {
        ShopEffectUnit shopEffectUnit = new ShopEffectUnit();
        shopEffectUnit.setShopId(shopId);
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
        return partyChange;
    }

    public static MachineEvent buildMessageShopDetailsChanged(Long sequenceId, String partyId, String shopId) {
        PartyChange partyChange = buildShopDetailsPartyChange(shopId);
        return buildMachineEvent(partyId, sequenceId, partyChange);
    }

    public static PartyChange buildShopDetailsPartyChange(String shopId) {
        ShopEffectUnit shopEffectUnit = new ShopEffectUnit();
        shopEffectUnit.setShopId(shopId);
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
        return partyChange;
    }

    public static MachineEvent buildMessageShopPayoutScheduleChanged(Long sequenceId, String partyId, String shopdId) {
        PartyChange partyChange = buildShopPayouScheduleChangedPartyChange(shopdId);
        return buildMachineEvent(partyId, sequenceId, partyChange);
    }

    public static PartyChange buildShopPayouScheduleChangedPartyChange(String shopdId) {
        ShopEffectUnit shopEffectUnit = new ShopEffectUnit();
        shopEffectUnit.setShopId(shopdId);
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
        return partyChange;
    }

    public static MachineEvent buildMessageShopPayoutToolChanged(Long sequenceId, String partyId, String shopdId) {
        PartyChange partyChange = buildShopPayoutToolChangedPartyChange(shopdId, PAYOUT_TOOL_ID);
        return buildMachineEvent(partyId, sequenceId, partyChange);
    }

    public static PartyChange buildShopPayoutToolChangedPartyChange(String shopdId, String payoutToolId) {
        ShopEffectUnit shopEffectUnit = new ShopEffectUnit();
        shopEffectUnit.setShopId(shopdId);
        ShopEffect shopEffect = new ShopEffect();
        shopEffect.setPayoutToolChanged(payoutToolId);
        shopEffectUnit.setEffect(shopEffect);
        ClaimEffect claimEffect = new ClaimEffect();
        claimEffect.setShopEffect(shopEffectUnit);
        Claim claim = buildClaimCreated(claimEffect);
        PartyChange partyChange = new PartyChange();
        partyChange.setClaimCreated(claim);
        return partyChange;
    }

    public static MachineEvent buildMessageShopAccountCreated(Long sequenceId, String partyId, String shopdId) {
        PartyChange partyChange = buildShopAccountCreatedPartyChange(shopdId);
        return buildMachineEvent(partyId, sequenceId, partyChange);
    }

    public static PartyChange buildShopAccountCreatedPartyChange(String shopdId) {
        ShopEffectUnit shopEffectUnit = new ShopEffectUnit();
        shopEffectUnit.setShopId(shopdId);
        ShopAccount shopAccount = new ShopAccount();
        shopAccount.setCurrency(new CurrencyRef(CURRENCY_SYMBOL));
        shopAccount.setPayout(SHOP_ACCOUNT_PAYOUT);
        shopAccount.setSettlement(SETTLEMENT_ID);
        ShopEffect shopEffect = new ShopEffect();
        shopEffect.setAccountCreated(shopAccount);
        shopEffectUnit.setEffect(shopEffect);
        ClaimEffect claimEffect = new ClaimEffect();
        claimEffect.setShopEffect(shopEffectUnit);
        Claim claim = buildClaimCreated(claimEffect);
        PartyChange partyChange = new PartyChange();
        partyChange.setClaimCreated(claim);
        return partyChange;
    }

    public static MachineEvent buildContractorCreated(Long sequenceId, PartyContractor partyContractor, String partyId) throws IOException {
        PartyChange partyChange = buildContractorCreatedPartyChange(partyContractor);
        return buildMachineEvent(partyId, sequenceId, partyChange);
    }

    public static MachineEvent buildContractCreated(Long sequenceId, Contract contract, String partyId) throws IOException {
        PartyChange partyChange = buildContractCreatedPartyChange(contract);
        return buildMachineEvent(partyId, sequenceId, partyChange);
    }

    public static MachineEvent buildContractCreatedWithShop(Long sequenceId, Contract contract, Shop shop, String partyId) throws IOException {
        PartyChange partyChange = buildContractCreatedWithShopCreatedPartyChange(contract, shop);
        return buildMachineEvent(partyId, sequenceId, partyChange);
    }

    public static MachineEvent buildContractorIdChange(Long sequenceId, Contract contract, String partyId, String contractorId) throws IOException {
        PartyChange partyChange = buildContractorChangeIdPartyChange(contract, contractorId);
        return buildMachineEvent(partyId, sequenceId, partyChange);
    }

    public static PartyChange buildContractorCreatedPartyChange(PartyContractor partyContractor) {
        ContractorEffectUnit contractorEffectUnit = new ContractorEffectUnit();
        contractorEffectUnit.setId(partyContractor.getId());
        ContractorEffect contractorEffect = new ContractorEffect();
        contractorEffect.setCreated(partyContractor);
        contractorEffectUnit.setEffect(contractorEffect);
        ClaimEffect claimEffect = new ClaimEffect();
        claimEffect.setContractorEffect(contractorEffectUnit);
        Claim claim = buildClaimCreated(claimEffect);
        PartyChange partyChange = new PartyChange();
        partyChange.setClaimCreated(claim);
        return partyChange;
    }

    public static PartyChange buildContractCreatedPartyChange(Contract contract) {
        ContractEffectUnit contractEffectUnit = new ContractEffectUnit();
        contractEffectUnit.setContractId(contract.getId());
        ContractEffect contractEffect = new ContractEffect();
        contractEffect.setCreated(contract);
        contractEffectUnit.setEffect(contractEffect);
        ClaimEffect claimEffect = new ClaimEffect();
        claimEffect.setContractEffect(contractEffectUnit);
        Claim claim = buildClaimCreated(claimEffect);
        PartyChange partyChange = new PartyChange();
        partyChange.setClaimCreated(claim);
        return partyChange;
    }

    public static PartyChange buildContractCreatedWithShopCreatedPartyChange(Contract contract, Shop shop) {
        ContractEffectUnit contractEffectUnit = new ContractEffectUnit();
        contractEffectUnit.setContractId(contract.getId());
        ContractEffect contractEffect = new ContractEffect();
        contractEffect.setCreated(contract);
        contractEffectUnit.setEffect(contractEffect);
        ClaimEffect claimEffect = new ClaimEffect();
        claimEffect.setContractEffect(contractEffectUnit);
        final ShopEffectUnit shopEffectUnit = new ShopEffectUnit();
        shopEffectUnit.setShopId(shop.getId());
        final ShopEffect effect = new ShopEffect();
        effect.setCreated(shop);
        shopEffectUnit.setEffect(effect);
        ClaimEffect claimEffectShop = new ClaimEffect();
        claimEffectShop.setShopEffect(shopEffectUnit);
        Claim claim = buildClaimChanged(List.of(claimEffect, claimEffectShop));
        PartyChange partyChange = new PartyChange();
        partyChange.setClaimCreated(claim);
        return partyChange;
    }

    public static PartyChange buildContractorChangeIdPartyChange(Contract contract, String contractorId) {
        ContractEffectUnit contractEffectUnit = new ContractEffectUnit();
        contractEffectUnit.setContractId(contract.getId());
        ContractEffect contractEffect = new ContractEffect();
        contractEffect.setContractorChanged(contractorId);
        contractEffectUnit.setEffect(contractEffect);
        ClaimEffect claimEffect = new ClaimEffect();
        claimEffect.setContractEffect(contractEffectUnit);
        Claim claim = buildClaimCreated(claimEffect);
        PartyChange partyChange = new PartyChange();
        partyChange.setClaimCreated(claim);
        return partyChange;
    }

    public static MachineEvent buildContractorIdentificationLevelChanged(Long sequenceId, String partyId, String contractId) {
        PartyChange partyChange = buildContractorIdentificationLevelChangedPartyChange(contractId);
        return buildMachineEvent(partyId, sequenceId, partyChange);
    }

    public static PartyChange buildContractorIdentificationLevelChangedPartyChange(String contractId) {
        ContractorEffectUnit contractorEffectUnit = new ContractorEffectUnit();
        contractorEffectUnit.setId(contractId);
        ContractorEffect contractorEffect = new ContractorEffect();
        contractorEffect.setIdentificationLevelChanged(ContractorIdentificationLevel.partial);
        contractorEffectUnit.setEffect(contractorEffect);
        ClaimEffect claimEffect = new ClaimEffect();
        claimEffect.setContractorEffect(contractorEffectUnit);
        Claim claim = buildClaimCreated(claimEffect);
        PartyChange partyChange = new PartyChange();
        partyChange.setClaimCreated(claim);
        return partyChange;
    }

    public static MachineEvent buildMultiPartyChange(Long sequenceId, String partyId, PartyChange customPartyChange) throws IOException {
        PartyChange partyCreatedChange = buildPartyCreatedPartyChange(partyId);
        PartyChange partyRevisionChange = buildPartyRevisionChangedPartyChange();
        PartyChange partyBlockingChange = buildPartyBlockingPartyChange();
        PartyChange contractorCreatedPartyChange = buildContractorCreatedPartyChange(buildPartyContractor());
        PartyChange identificationLevelChangedPartyChange = buildContractorIdentificationLevelChangedPartyChange(CONTRACTOR_ID);
        if (customPartyChange == null) {
            return buildMachineEvent(partyId, sequenceId, partyCreatedChange, partyRevisionChange,
                    partyBlockingChange, contractorCreatedPartyChange, identificationLevelChangedPartyChange);
        } else {
            return buildMachineEvent(partyId, sequenceId, partyCreatedChange, partyRevisionChange,
                    partyBlockingChange, contractorCreatedPartyChange, identificationLevelChangedPartyChange, customPartyChange);
        }
    }

    public static PartyContractor buildPartyContractor() throws IOException {
        PartyContractor partyContractor = new PartyContractor();
        partyContractor.setId(CONTRACTOR_ID);
        partyContractor.setStatus(ContractorIdentificationLevel.full);
        Contractor contractor = Contractor.legal_entity(new LegalEntity());
        contractor = new MockTBaseProcessor(MockMode.ALL).process(contractor, new TBaseHandler<>(Contractor.class));
        partyContractor.setContractor(contractor);
        partyContractor.setIdentityDocuments(Collections.emptyList());
        return partyContractor;
    }

    public static Contract buildContract(String contractId) throws IOException {
        Contract contract = new Contract();
        Contractor contractor = new Contractor();
        contractor = new MockTBaseProcessor(MockMode.ALL).process(contractor, new TBaseHandler<>(Contractor.class));
        contract.setContractor(contractor);
        contract.setId(contractId);
        contract.setCreatedAt(LocalDateTime.now().toString());
        contract.setStatus(ContractStatus.active(new ContractActive()));
        contract.setTerms(new TermSetHierarchyRef());
        contract.setAdjustments(List.of());
        contract.setPayoutTools(List.of());
        contract.setContractorId(null);
        return contract;
    }

    public static PartyContractor buildRussianLegalPartyContractor() throws IOException {
        PartyContractor partyContractor = new PartyContractor();
        partyContractor.setId(CONTRACTOR_ID);
        partyContractor.setStatus(ContractorIdentificationLevel.none);
        Contractor contractor = new Contractor();
        LegalEntity legalEntity = new LegalEntity();
        RussianLegalEntity russianLegalEntity = new RussianLegalEntity();
        russianLegalEntity = new MockTBaseProcessor(MockMode.ALL).process(russianLegalEntity, new TBaseHandler<>(RussianLegalEntity.class));
        russianLegalEntity.setInn(INN);
        legalEntity.setRussianLegalEntity(russianLegalEntity);
        contractor.setLegalEntity(legalEntity);
        partyContractor.setContractor(contractor);
        partyContractor.setIdentityDocuments(Collections.emptyList());
        return partyContractor;
    }

    public static Claim buildClaimCreated(ClaimEffect claimEffect) {
        ClaimAccepted claimAccepted = new ClaimAccepted();
        claimAccepted.setEffects(Collections.singletonList(claimEffect));
        ClaimStatus claimStatus = ClaimStatus.accepted(claimAccepted);
        return new Claim(CLAIM_ID, claimStatus, Collections.emptyList(), REVISION_ID, TypeUtil.temporalToString(LocalDateTime.now()));
    }

    public static Claim buildClaimChanged(List<ClaimEffect> claimEffects) {
        ClaimAccepted claimAccepted = new ClaimAccepted();
        claimAccepted.setEffects(claimEffects);
        ClaimStatus claimStatus = ClaimStatus.accepted(claimAccepted);
        return new Claim(CLAIM_ID, claimStatus, Collections.emptyList(), REVISION_ID, TypeUtil.temporalToString(LocalDateTime.now()));
    }

    public static Shop buildShopCreated(String contractId) throws IOException {
        Shop shop = new Shop();
        shop = new MockTBaseProcessor(MockMode.ALL).process(shop, new TBaseHandler<>(Shop.class));
        shop.setCreatedAt(TypeUtil.temporalToString(LocalDateTime.now()));
        Blocking blocking = new Blocking();
        blocking.setBlocked(new Blocked(SHOP_BLOCK_REASON, TypeUtil.temporalToString(LocalDateTime.now())));
        shop.setBlocking(blocking);
        shop.setContractId(contractId);
        shop.setSuspension(buildPartySuspension());
        return shop;
    }

    public static ShopSuspension buildActiveShopSuspension(String since, String shopId) {
        Suspension suspension = new Suspension();
        suspension.setActive(new Active(since));
        return new ShopSuspension(shopId, suspension);
    }

    public static ShopSuspension buildSuspendedShopSuspension(String since, String shopId) {
        Suspension suspension = new Suspension();
        suspension.setSuspended(new Suspended(since));
        return new ShopSuspension(shopId, suspension);
    }

    public static ShopBlocking buildShopBlocking(String shopId) {
        Blocking blocking = new Blocking();
        blocking.setUnblocked(new Unblocked(SHOP_UNBLOCK_REASON, TypeUtil.temporalToString(LocalDateTime.now())));
        return new ShopBlocking(shopId, blocking);
    }

    public static PartyRevisionChanged buildPartyRevisionChanged() {
        return new PartyRevisionChanged(TypeUtil.temporalToString(LocalDateTime.now()), PARTY_REVISION_ID);
    }

    public static PartyCreated buildPartyCreated(String partyId) {
        return new PartyCreated(partyId, new PartyContactInfo(PARTY_EMAIL), TypeUtil.temporalToString(LocalDateTime.now()));
    }

    public static Suspension buildPartySuspension() {
        Suspension suspension = new Suspension();
        suspension.setActive(new Active(TypeUtil.temporalToString(LocalDateTime.now())));

        return suspension;
    }

    public static Blocking buildPartyBlocking() {
        Blocking blocking = new Blocking();
        blocking.setBlocked(new Blocked(PARTY_BLOCK_REASON, TypeUtil.temporalToString(LocalDateTime.now())));
        return blocking;
    }

    public static MachineEvent buildMachineEvent(String sourceId, Long sequenceId, PartyChange... partyChange) {
        MachineEvent message = new MachineEvent();
        ArrayList<PartyChange> partyChanges = new ArrayList<>(Arrays.asList(partyChange));

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

    public static SinkEvent buildSinkEvent(MachineEvent machineEvent) {
        SinkEvent sinkEvent = new SinkEvent();
        sinkEvent.setEvent(machineEvent);
        return sinkEvent;
    }

}
