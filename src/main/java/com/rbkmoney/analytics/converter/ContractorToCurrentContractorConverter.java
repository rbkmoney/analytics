package com.rbkmoney.analytics.converter;

import com.rbkmoney.damsel.domain.Contractor;
import com.rbkmoney.damsel.domain.InternationalLegalEntity;
import com.rbkmoney.damsel.domain.RussianLegalEntity;
import com.rbkmoney.damsel.domain.RussianPrivateEntity;
import com.rbkmoney.geck.common.util.TBaseUtil;
import org.springframework.core.convert.converter.Converter;
import org.springframework.stereotype.Component;

@Component
public class ContractorToCurrentContractorConverter implements Converter<Contractor,
        com.rbkmoney.analytics.domain.db.tables.pojos.Contractor> {

    @Override
    public com.rbkmoney.analytics.domain.db.tables.pojos.Contractor convert(Contractor contractor) {
        com.rbkmoney.analytics.domain.db.tables.pojos.Contractor currentContractor =
                new com.rbkmoney.analytics.domain.db.tables.pojos.Contractor();
        currentContractor.setContractorType(TBaseUtil.unionFieldToEnum(contractor,
                com.rbkmoney.analytics.domain.db.enums.ContractorType.class));
        if (contractor.isSetRegisteredUser()) {
            currentContractor.setRegUserEmail(contractor.getRegisteredUser().getEmail());
        } else if (contractor.isSetLegalEntity()) {
            currentContractor.setLegalEntityType(TBaseUtil.unionFieldToEnum(contractor.getLegalEntity(),
                    com.rbkmoney.analytics.domain.db.enums.LegalEntity.class));
            if (contractor.getLegalEntity().isSetRussianLegalEntity()) {
                RussianLegalEntity russianLegalEntity = contractor.getLegalEntity().getRussianLegalEntity();
                currentContractor.setRussianLegalEntityName(russianLegalEntity.getRegisteredName());
                currentContractor.setRussianLegalEntityRegisteredNumber(russianLegalEntity.getRegisteredNumber());
                currentContractor.setRussianLegalEntityInn(russianLegalEntity.getInn());
                currentContractor.setRussianLegalEntityActualAddress(russianLegalEntity.getActualAddress());
                currentContractor.setRussianLegalEntityPostAddress(russianLegalEntity.getPostAddress());
                currentContractor.setRussianLegalEntityRepresentativePosition(
                        russianLegalEntity.getRepresentativePosition());
                currentContractor.setRussianLegalEntityRepresentativeFullName(
                        russianLegalEntity.getRepresentativeFullName());
                currentContractor.setRussianLegalEntityRepresentativeDocument(
                        russianLegalEntity.getRepresentativeDocument());
                currentContractor.setRussianLegalEntityBankAccount(
                        russianLegalEntity.getRussianBankAccount().getAccount());
                currentContractor.setRussianLegalEntityBankName(
                        russianLegalEntity.getRussianBankAccount().getBankName());
                currentContractor.setRussianLegalEntityBankPostAccount(
                        russianLegalEntity.getRussianBankAccount().getBankPostAccount());
                currentContractor.setRussianLegalEntityBankBik(russianLegalEntity.getRussianBankAccount().getBankBik());
            } else if (contractor.getLegalEntity().isSetInternationalLegalEntity()) {
                InternationalLegalEntity internationalLegalEntity =
                        contractor.getLegalEntity().getInternationalLegalEntity();
                currentContractor.setInternationalLegalEntityName(internationalLegalEntity.getLegalName());
                currentContractor.setInternationalLegalEntityTradingName(internationalLegalEntity.getTradingName());
                currentContractor.setInternationalLegalEntityRegisteredAddress(
                        internationalLegalEntity.getRegisteredAddress());
                currentContractor.setInternationalActualAddress(internationalLegalEntity.getActualAddress());
                currentContractor.setInternationalLegalEntityRegisteredNumber(
                        internationalLegalEntity.getRegisteredNumber());
            }
        } else if (contractor.isSetPrivateEntity()) {
            currentContractor.setPrivateEntityType(TBaseUtil.unionFieldToEnum(contractor.getPrivateEntity(),
                    com.rbkmoney.analytics.domain.db.enums.PrivateEntity.class)
            );
            if (contractor.getPrivateEntity().isSetRussianPrivateEntity()) {
                RussianPrivateEntity russianPrivateEntity = contractor.getPrivateEntity().getRussianPrivateEntity();
                if (russianPrivateEntity.getContactInfo() != null) {
                    currentContractor.setRussianPrivateEntityEmail(russianPrivateEntity.getContactInfo().getEmail());
                    currentContractor.setRussianPrivateEntityPhoneNumber(russianPrivateEntity
                            .getContactInfo().getPhoneNumber());
                }
                currentContractor.setRussianPrivateEntityFirstName(russianPrivateEntity.getFirstName());
                currentContractor.setRussianPrivateEntitySecondName(russianPrivateEntity.getSecondName());
                currentContractor.setRussianPrivateEntityMiddleName(russianPrivateEntity.getMiddleName());
            }
        }

        return currentContractor;
    }

}
