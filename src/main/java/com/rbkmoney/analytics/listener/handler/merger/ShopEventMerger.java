package com.rbkmoney.analytics.listener.handler.merger;

import com.rbkmoney.analytics.dao.repository.postgres.party.management.ShopDao;
import com.rbkmoney.analytics.domain.db.tables.pojos.Shop;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class ShopEventMerger {

    private final ShopDao shopDao;

    public Shop mergeShop(String partyId, String shopId, Shop shop, Shop targetShop) {
        return mergeShops(partyId, shopId, shop, targetShop);
    }

    public Shop mergeShop(String partyId, String shopId, Shop shop) {
        Shop targetShop = shopDao.getShopByPartyIdAndShopId(partyId, shopId);
        return mergeShops(partyId, shopId, shop, targetShop);
    }

    @NotNull
    private Shop mergeShops(String partyId, String shopId, Shop shop, Shop targetShop) {
        if (targetShop == null) {
            targetShop = new Shop();
        }
        targetShop.setPartyId(partyId);
        targetShop.setShopId(shopId);
        targetShop.setEventId(shop.getEventId());
        targetShop.setEventTime(shop.getEventTime());
        targetShop.setCategoryId(shop.getCategoryId() != null ? shop.getCategoryId() : targetShop.getCategoryId());
        targetShop.setContractId(shop.getContractId() != null ? shop.getContractId() : targetShop.getContractId());
        targetShop.setPayoutToolId(shop.getPayoutToolId() != null
                ? shop.getPayoutToolId() : targetShop.getPayoutToolId());
        targetShop.setPayoutScheduleId(shop.getPayoutScheduleId() != null
                ? shop.getPayoutScheduleId() : targetShop.getPayoutScheduleId());
        targetShop.setCreatedAt(shop.getCreatedAt() != null ? shop.getCreatedAt() : targetShop.getCreatedAt());
        targetShop.setBlocking(shop.getBlocking() != null ? shop.getBlocking() : targetShop.getBlocking());
        targetShop.setBlockedReason(shop.getBlockedReason() != null
                ? shop.getBlockedReason() : targetShop.getBlockedReason());
        targetShop.setBlockedSince(shop.getBlockedSince() != null
                ? shop.getBlockedSince() : targetShop.getBlockedSince());
        targetShop.setUnblockedReason(shop.getUnblockedReason() != null
                ? shop.getUnblockedReason() : targetShop.getUnblockedReason());
        targetShop.setUnblockedSince(shop.getUnblockedSince() != null
                ? shop.getUnblockedSince() : targetShop.getUnblockedSince());
        targetShop.setSuspension(shop.getSuspension() != null ? shop.getSuspension() : targetShop.getSuspension());
        targetShop.setSuspensionActiveSince(shop.getSuspensionActiveSince() != null
                ? shop.getSuspensionActiveSince() : targetShop.getSuspensionActiveSince());
        targetShop.setSuspensionSuspendedSince(shop.getSuspensionSuspendedSince() != null
                ? shop.getSuspensionSuspendedSince() : targetShop.getSuspensionSuspendedSince());
        targetShop.setDetailsName(shop.getDetailsName() != null
                ? shop.getDetailsName() : targetShop.getDetailsName());
        targetShop.setDetailsDescription(shop.getDetailsDescription() != null
                ? shop.getDetailsDescription() : targetShop.getDetailsDescription());
        targetShop.setLocationUrl(shop.getLocationUrl() != null ? shop.getLocationUrl() : targetShop.getLocationUrl());
        targetShop.setAccountCurrencyCode(shop.getAccountCurrencyCode() != null
                ? shop.getAccountCurrencyCode() : targetShop.getAccountCurrencyCode());
        targetShop.setAccountSettlement(shop.getAccountSettlement() != null
                ? shop.getAccountSettlement() : targetShop.getAccountSettlement());
        targetShop.setAccountGuarantee(shop.getAccountGuarantee() != null
                ? shop.getAccountGuarantee() : targetShop.getAccountGuarantee());
        targetShop.setAccountPayout(shop.getAccountPayout() != null
                ? shop.getAccountPayout() : targetShop.getAccountPayout());
        targetShop.setContractorId(shop.getContractorId() != null
                ? shop.getContractorId() : targetShop.getContractorId());
        targetShop.setContractorType(shop.getContractorType() != null
                ? shop.getContractorType() : targetShop.getContractorType());
        targetShop.setRegUserEmail(shop.getRegUserEmail() != null
                ? shop.getRegUserEmail() : targetShop.getRegUserEmail());
        targetShop.setLegalEntityType(shop.getLegalEntityType() != null
                ? shop.getLegalEntityType() : targetShop.getLegalEntityType());
        targetShop.setRussianLegalEntityInn(shop.getRussianLegalEntityInn() != null
                ? shop.getRussianLegalEntityInn() : targetShop.getRussianLegalEntityInn());
        targetShop.setRussianLegalEntityName(shop.getRussianLegalEntityName() != null
                ? shop.getRussianLegalEntityName() : targetShop.getRussianLegalEntityName());
        targetShop.setRussianLegalEntityRegisteredNumber(shop.getRussianLegalEntityRegisteredNumber() != null
                ? shop.getRussianLegalEntityRegisteredNumber() : targetShop.getRussianLegalEntityRegisteredNumber());
        targetShop.setRussianLegalEntityActualAddress(shop.getRussianLegalEntityActualAddress() != null
                ? shop.getRussianLegalEntityActualAddress() : targetShop.getRussianLegalEntityActualAddress());
        targetShop.setRussianLegalEntityPostAddress(shop.getRussianLegalEntityPostAddress() != null
                ? shop.getRussianLegalEntityPostAddress() : targetShop.getRussianLegalEntityPostAddress());
        targetShop.setRussianLegalEntityRepresentativePosition(
                shop.getRussianLegalEntityRepresentativePosition() != null
                        ? shop.getRussianLegalEntityRepresentativePosition()
                        : targetShop.getRussianLegalEntityRepresentativePosition());
        targetShop.setRussianLegalEntityRepresentativeDocument(
                shop.getRussianLegalEntityRepresentativeDocument() != null
                        ? shop.getRussianLegalEntityRepresentativeDocument()
                        : targetShop.getRussianLegalEntityRepresentativeDocument());
        targetShop.setRussianLegalEntityRepresentativeFullName(
                shop.getRussianLegalEntityRepresentativeFullName() != null
                        ? shop.getRussianLegalEntityRepresentativeFullName()
                        : targetShop.getRussianLegalEntityRepresentativeFullName());
        targetShop.setRussianLegalEntityBankAccount(shop.getRussianLegalEntityBankAccount() != null
                ? shop.getRussianLegalEntityBankAccount() : targetShop.getRussianLegalEntityBankAccount());
        targetShop.setRussianLegalEntityBankBik(shop.getRussianLegalEntityBankBik() != null
                ? shop.getRussianLegalEntityBankBik() : targetShop.getRussianLegalEntityBankBik());
        targetShop.setRussianLegalEntityBankName(shop.getRussianLegalEntityBankName() != null
                ? shop.getRussianLegalEntityBankName() : targetShop.getRussianLegalEntityBankName());
        targetShop.setRussianLegalEntityBankPostAccount(shop.getRussianLegalEntityBankPostAccount() != null
                ? shop.getRussianLegalEntityBankPostAccount() : targetShop.getRussianLegalEntityBankPostAccount());
        targetShop.setInternationalLegalEntityName(shop.getInternationalLegalEntityName() != null
                ? shop.getInternationalLegalEntityName() : targetShop.getInternationalLegalEntityName());
        targetShop.setInternationalLegalEntityRegisteredAddress(
                shop.getInternationalLegalEntityRegisteredAddress() != null
                        ? shop.getInternationalLegalEntityRegisteredAddress()
                        : targetShop.getInternationalLegalEntityRegisteredAddress());
        targetShop.setInternationalLegalEntityRegisteredNumber(
                shop.getInternationalLegalEntityRegisteredNumber() != null
                        ? shop.getInternationalLegalEntityRegisteredNumber()
                        : targetShop.getInternationalLegalEntityRegisteredNumber());
        targetShop.setInternationalLegalEntityTradingName(shop.getInternationalLegalEntityTradingName() != null
                ? shop.getInternationalLegalEntityTradingName() : targetShop.getInternationalLegalEntityTradingName());
        targetShop.setPrivateEntityType(shop.getPrivateEntityType() != null
                ? shop.getPrivateEntityType() : targetShop.getPrivateEntityType());
        targetShop.setRussianPrivateEntityEmail(shop.getRussianPrivateEntityEmail() != null
                ? shop.getRussianPrivateEntityEmail() : targetShop.getRussianPrivateEntityEmail());
        targetShop.setRussianPrivateEntityFirstName(shop.getRussianPrivateEntityFirstName() != null
                ? shop.getRussianPrivateEntityFirstName() : targetShop.getRussianPrivateEntityFirstName());
        targetShop.setRussianPrivateEntitySecondName(shop.getRussianPrivateEntitySecondName() != null
                ? shop.getRussianPrivateEntitySecondName() : targetShop.getRussianPrivateEntitySecondName());
        targetShop.setRussianPrivateEntityMiddleName(shop.getRussianPrivateEntityMiddleName() != null
                ? shop.getRussianPrivateEntityMiddleName() : targetShop.getRussianPrivateEntityMiddleName());
        targetShop.setRussianPrivateEntityPhoneNumber(shop.getRussianPrivateEntityPhoneNumber() != null
                ? shop.getRussianPrivateEntityPhoneNumber() : targetShop.getRussianPrivateEntityPhoneNumber());
        targetShop.setContractorIdentificationLevel(shop.getContractorIdentificationLevel() != null
                ? shop.getContractorIdentificationLevel() : targetShop.getContractorIdentificationLevel());

        log.debug("ShopEventMerger target shop: {}", targetShop);
        return targetShop;
    }
}
