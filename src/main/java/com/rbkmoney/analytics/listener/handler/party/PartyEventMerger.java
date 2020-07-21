package com.rbkmoney.analytics.listener.handler.party;

import com.rbkmoney.analytics.domain.db.tables.pojos.Party;
import com.rbkmoney.analytics.domain.db.tables.pojos.Shop;
import com.rbkmoney.analytics.service.PartyService;
import com.rbkmoney.analytics.service.model.ShopKey;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@RequiredArgsConstructor
public class PartyEventMerger {

    private final PartyService partyService;

    public Party mergeParty(String partyId, List<Party> parties) {
        Party targetParty = partyService.getParty(partyId);
        if (targetParty == null) {
            targetParty = new Party();
        }
        for (Party party : parties) {
            targetParty.setPartyId(partyId);
            targetParty.setEventId(party.getEventId());
            targetParty.setEventTime(party.getEventTime());
            targetParty.setCreatedAt(party.getCreatedAt() != null ? party.getCreatedAt() : targetParty.getCreatedAt());
            targetParty.setEmail(party.getEmail() != null ? party.getEmail() : targetParty.getEmail());
            targetParty.setBlocking(party.getBlocking() != null ? party.getBlocking() : targetParty.getBlocking());
            targetParty.setBlockedReason(party.getBlockedReason() != null ? party.getBlockedReason() : targetParty.getBlockedReason());
            targetParty.setBlockedSince(party.getBlockedSince() != null ? party.getBlockedSince() : targetParty.getBlockedSince());
            targetParty.setUnblockedReason(party.getUnblockedReason() != null ? party.getUnblockedReason() : targetParty.getUnblockedReason());
            targetParty.setUnblockedSince(party.getUnblockedSince() != null ? party.getUnblockedSince() : targetParty.getUnblockedSince());
            targetParty.setSuspension(party.getSuspension() != null ? party.getSuspension() : targetParty.getSuspension());
            targetParty.setSuspensionActiveSince(party.getSuspensionActiveSince() != null ? party.getSuspensionActiveSince() : targetParty.getSuspensionActiveSince());
            targetParty.setRevisionId(party.getRevisionId() != null ? party.getRevisionId() : targetParty.getRevisionId());
            targetParty.setRevisionChangedAt(party.getRevisionChangedAt() != null ? party.getRevisionChangedAt() : targetParty.getRevisionChangedAt());
            targetParty.setContractorId(party.getContractorId() != null ? party.getContractorId() : targetParty.getContractorId());
            targetParty.setContractorType(party.getContractorType() != null ? party.getContractorType() : targetParty.getContractorType());
            targetParty.setRegUserEmail(party.getRegUserEmail() != null ? party.getRegUserEmail() : targetParty.getRegUserEmail());
            targetParty.setLegalEntityType(party.getLegalEntityType() != null ? party.getLegalEntityType() : targetParty.getLegalEntityType());
            targetParty.setRussianLegalEntityInn(party.getRussianLegalEntityInn() != null ? party.getRussianLegalEntityInn() : targetParty.getRussianLegalEntityInn());
            targetParty.setRussianLegalEntityName(party.getRussianLegalEntityName() != null ? party.getRussianLegalEntityName() : targetParty.getRussianLegalEntityName());
            targetParty.setRussianLegalEntityRegisteredNumber(party.getRussianLegalEntityRegisteredNumber() != null ? party.getRussianLegalEntityRegisteredNumber() : targetParty.getRussianLegalEntityRegisteredNumber());
            targetParty.setRussianLegalEntityActualAddress(party.getRussianLegalEntityActualAddress() != null ? party.getRussianLegalEntityActualAddress() : targetParty.getRussianLegalEntityActualAddress());
            targetParty.setRussianLegalEntityPostAddress(party.getRussianLegalEntityPostAddress() != null ? party.getRussianLegalEntityPostAddress() : targetParty.getRussianLegalEntityPostAddress());
            targetParty.setRussianLegalEntityRepresentativePosition(party.getRussianLegalEntityRepresentativePosition() != null ? party.getRussianLegalEntityRepresentativePosition() : targetParty.getRussianLegalEntityRepresentativePosition());
            targetParty.setRussianLegalEntityRepresentativeDocument(party.getRussianLegalEntityRepresentativeDocument() != null ? party.getRussianLegalEntityRepresentativeDocument() : targetParty.getRussianLegalEntityRepresentativeDocument());
            targetParty.setRussianLegalEntityRepresentativeFullName(party.getRussianLegalEntityRepresentativeFullName() != null ? party.getRussianLegalEntityRepresentativeFullName() : targetParty.getRussianLegalEntityRepresentativeFullName());
            targetParty.setRussianLegalEntityBankAccount(party.getRussianLegalEntityBankAccount() != null ? party.getRussianLegalEntityBankAccount() : targetParty.getRussianLegalEntityBankAccount());
            targetParty.setRussianLegalEntityBankBik(party.getRussianLegalEntityBankBik() != null ? party.getRussianLegalEntityBankBik() : targetParty.getRussianLegalEntityBankBik());
            targetParty.setRussianLegalEntityBankName(party.getRussianLegalEntityBankName() != null ? party.getRussianLegalEntityBankName() : targetParty.getRussianLegalEntityBankName());
            targetParty.setRussianLegalEntityBankPostAccount(party.getRussianLegalEntityBankPostAccount() != null ? party.getRussianLegalEntityBankPostAccount() : targetParty.getRussianLegalEntityBankPostAccount());
            targetParty.setInternationalLegalEntityName(party.getInternationalLegalEntityName() != null ? party.getInternationalLegalEntityName() : targetParty.getInternationalLegalEntityName());
            targetParty.setInternationalLegalEntityRegisteredAddress(party.getInternationalLegalEntityRegisteredAddress() != null ? party.getInternationalLegalEntityRegisteredAddress() : targetParty.getInternationalLegalEntityRegisteredAddress());
            targetParty.setInternationalLegalEntityRegisteredNumber(party.getInternationalLegalEntityRegisteredNumber() != null ? party.getInternationalLegalEntityRegisteredNumber() : targetParty.getInternationalLegalEntityRegisteredNumber());
            targetParty.setInternationalLegalEntityTradingName(party.getInternationalLegalEntityTradingName() != null ? party.getInternationalLegalEntityTradingName() : targetParty.getInternationalLegalEntityTradingName());
            targetParty.setPrivateEntityType(party.getPrivateEntityType() != null ? party.getPrivateEntityType() : targetParty.getPrivateEntityType());
            targetParty.setRussianPrivateEntityEmail(party.getRussianPrivateEntityEmail() != null ? party.getRussianPrivateEntityEmail() : targetParty.getRussianPrivateEntityEmail());
            targetParty.setRussianPrivateEntityFirstName(party.getRussianPrivateEntityFirstName() != null ? party.getRussianPrivateEntityFirstName() : targetParty.getRussianPrivateEntityFirstName());
            targetParty.setRussianPrivateEntitySecondName(party.getRussianPrivateEntitySecondName() != null ? party.getRussianPrivateEntitySecondName() : targetParty.getRussianPrivateEntitySecondName());
            targetParty.setRussianPrivateEntityMiddleName(party.getRussianPrivateEntityMiddleName() != null ? party.getRussianPrivateEntityMiddleName() : targetParty.getRussianPrivateEntityMiddleName());
            targetParty.setRussianPrivateEntityPhoneNumber(party.getRussianPrivateEntityPhoneNumber() != null ? party.getRussianPrivateEntityPhoneNumber() : targetParty.getRussianPrivateEntityPhoneNumber());
            targetParty.setContractorIdentificationLevel(party.getContractorIdentificationLevel() != null ?
                    party.getContractorIdentificationLevel() : targetParty.getContractorIdentificationLevel());
        }

        return targetParty;
    }

    public Shop mergeShop(ShopKey key, List<Shop> shops) {
        Shop targetShop = partyService.getShop(key);
        if (targetShop == null) {
            targetShop = new Shop();
        }
        for (Shop shop : shops) {
            targetShop.setPartyId(key.getPartyId());
            targetShop.setShopId(key.getShopId());
            targetShop.setEventId(shop.getEventId());
            targetShop.setEventTime(shop.getEventTime());
            targetShop.setCategoryId(shop.getCategoryId() != null ? shop.getCategoryId() : targetShop.getCategoryId());
            targetShop.setContractId(shop.getContractId() != null ? shop.getContractId() : targetShop.getContractId());
            targetShop.setPayoutToolId(shop.getPayoutToolId() != null ? shop.getPayoutToolId() : targetShop.getPayoutToolId());
            targetShop.setPayoutScheduleId(shop.getPayoutScheduleId() != null ? shop.getPayoutScheduleId() : targetShop.getPayoutScheduleId());
            targetShop.setCreatedAt(shop.getCreatedAt() != null ? shop.getCreatedAt() : targetShop.getCreatedAt());
            targetShop.setBlocking(shop.getBlocking() != null ? shop.getBlocking() : targetShop.getBlocking());
            targetShop.setBlockedReason(shop.getBlockedReason() != null ? shop.getBlockedReason() : targetShop.getBlockedReason());
            targetShop.setBlockedSince(shop.getBlockedSince() != null ? shop.getBlockedSince() : targetShop.getBlockedSince());
            targetShop.setUnblockedReason(shop.getUnblockedReason() != null ? shop.getUnblockedReason() : targetShop.getUnblockedReason());
            targetShop.setUnblockedSince(shop.getUnblockedSince() != null ? shop.getUnblockedSince() : targetShop.getUnblockedSince());
            targetShop.setSuspension(shop.getSuspension() != null ? shop.getSuspension() : targetShop.getSuspension());
            targetShop.setSuspensionActiveSince(shop.getSuspensionActiveSince() != null ? shop.getSuspensionActiveSince() : targetShop.getSuspensionActiveSince());
            targetShop.setSuspensionSuspendedSince(shop.getSuspensionSuspendedSince() != null ? shop.getSuspensionSuspendedSince() : targetShop.getSuspensionSuspendedSince());
            targetShop.setDetailsName(shop.getDetailsName() != null ? shop.getDetailsName() : targetShop.getDetailsName());
            targetShop.setDetailsDescription(shop.getDetailsDescription() != null ? shop.getDetailsDescription() : targetShop.getDetailsDescription());
            targetShop.setLocationUrl(shop.getLocationUrl() != null ? shop.getLocationUrl() : targetShop.getLocationUrl());
            targetShop.setAccountCurrencyCode(shop.getAccountCurrencyCode() != null ? shop.getAccountCurrencyCode() : targetShop.getAccountCurrencyCode());
            targetShop.setAccountSettlement(shop.getAccountSettlement() != null ? shop.getAccountSettlement() : targetShop.getAccountSettlement());
            targetShop.setAccountGuarantee(shop.getAccountGuarantee() != null ? shop.getAccountGuarantee() : targetShop.getAccountGuarantee());
            targetShop.setAccountPayout(shop.getAccountPayout() != null ? shop.getAccountPayout() : targetShop.getAccountPayout());
        }

        return targetShop;
    }
}
