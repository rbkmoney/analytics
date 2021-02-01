package com.rbkmoney.analytics.converter;

import com.rbkmoney.analytics.domain.db.tables.pojos.Contractor;
import com.rbkmoney.analytics.domain.db.tables.pojos.Shop;
import org.springframework.core.convert.converter.Converter;
import org.springframework.stereotype.Component;

@Component
public class ContractorToShopConverter implements Converter<Contractor, Shop> {

    @Override
    public Shop convert(Contractor contractor) {
        Shop shop = new Shop();
        shop.setContractorType(contractor.getContractorType());
        shop.setRegUserEmail(contractor.getRegUserEmail());
        shop.setLegalEntityType(contractor.getLegalEntityType());
        shop.setRussianLegalEntityName(contractor.getRussianLegalEntityName());
        shop.setRussianLegalEntityRegisteredNumber(contractor.getRussianLegalEntityRegisteredNumber());
        shop.setRussianLegalEntityInn(contractor.getRussianLegalEntityInn());
        shop.setRussianLegalEntityActualAddress(contractor.getRussianLegalEntityActualAddress());
        shop.setRussianLegalEntityPostAddress(contractor.getRussianLegalEntityPostAddress());
        shop.setRussianLegalEntityRepresentativePosition(contractor.getRussianLegalEntityRepresentativePosition());
        shop.setRussianLegalEntityRepresentativeFullName(contractor.getRussianLegalEntityRepresentativeFullName());
        shop.setRussianLegalEntityRepresentativeDocument(contractor.getRussianLegalEntityRepresentativeDocument());
        shop.setRussianLegalEntityBankAccount(contractor.getRussianLegalEntityBankAccount());
        shop.setRussianLegalEntityBankName(contractor.getRussianLegalEntityBankName());
        shop.setRussianLegalEntityBankPostAccount(contractor.getRussianLegalEntityBankPostAccount());
        shop.setRussianLegalEntityBankBik(contractor.getRussianLegalEntityBankBik());
        shop.setInternationalLegalEntityName(contractor.getInternationalLegalEntityName());
        shop.setInternationalLegalEntityTradingName(contractor.getInternationalLegalEntityTradingName());
        shop.setInternationalLegalEntityRegisteredAddress(contractor.getInternationalLegalEntityRegisteredAddress());
        shop.setInternationalActualAddress(contractor.getInternationalActualAddress());
        shop.setInternationalLegalEntityRegisteredNumber(contractor.getInternationalLegalEntityRegisteredNumber());
        shop.setPrivateEntityType(contractor.getPrivateEntityType());
        shop.setRussianPrivateEntityEmail(contractor.getRussianPrivateEntityEmail());
        shop.setRussianPrivateEntityPhoneNumber(contractor.getRussianPrivateEntityPhoneNumber());
        shop.setRussianPrivateEntityFirstName(contractor.getRussianPrivateEntityFirstName());
        shop.setRussianPrivateEntitySecondName(contractor.getRussianPrivateEntitySecondName());
        shop.setRussianPrivateEntityMiddleName(contractor.getRussianPrivateEntityMiddleName());
        return shop;
    }

}
