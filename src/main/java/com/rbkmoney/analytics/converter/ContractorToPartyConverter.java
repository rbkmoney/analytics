package com.rbkmoney.analytics.converter;

import com.rbkmoney.analytics.domain.db.tables.pojos.Party;
import com.rbkmoney.damsel.domain.Contractor;
import com.rbkmoney.damsel.domain.InternationalLegalEntity;
import com.rbkmoney.damsel.domain.RussianLegalEntity;
import com.rbkmoney.damsel.domain.RussianPrivateEntity;
import com.rbkmoney.geck.common.util.TBaseUtil;
import org.springframework.core.convert.converter.Converter;
import org.springframework.stereotype.Component;

@Component
public class ContractorToPartyConverter implements Converter<Contractor, Party> {

    @Override
    public Party convert(Contractor contractor) {
        Party party = new Party();
        party.setContractorType(TBaseUtil.unionFieldToEnum(contractor, com.rbkmoney.analytics.domain.db.enums.Contractor.class));
        if (contractor.isSetRegisteredUser()) {
            party.setRegUserEmail(contractor.getRegisteredUser().getEmail());
        } else if (contractor.isSetLegalEntity()) {
            party.setLegalEntityType(TBaseUtil.unionFieldToEnum(contractor.getLegalEntity(), com.rbkmoney.analytics.domain.db.enums.LegalEntity.class));
            if (contractor.getLegalEntity().isSetRussianLegalEntity()) {
                RussianLegalEntity russianLegalEntity = contractor.getLegalEntity().getRussianLegalEntity();
                party.setRussianLegalEntityName(russianLegalEntity.getRegisteredName());
                party.setRussianLegalEntityRegisteredNumber(russianLegalEntity.getRegisteredNumber());
                party.setRussianLegalEntityInn(russianLegalEntity.getInn());
                party.setRussianLegalEntityActualAddress(russianLegalEntity.getActualAddress());
                party.setRussianLegalEntityPostAddress(russianLegalEntity.getPostAddress());
                party.setRussianLegalEntityRepresentativePosition(russianLegalEntity.getRepresentativePosition());
                party.setRussianLegalEntityRepresentativeFullName(russianLegalEntity.getRepresentativeFullName());
                party.setRussianLegalEntityRepresentativeDocument(russianLegalEntity.getRepresentativeDocument());
                party.setRussianLegalEntityBankAccount(russianLegalEntity.getRussianBankAccount().getAccount());
                party.setRussianLegalEntityBankName(russianLegalEntity.getRussianBankAccount().getBankName());
                party.setRussianLegalEntityBankPostAccount(russianLegalEntity.getRussianBankAccount().getBankPostAccount());
                party.setRussianLegalEntityBankBik(russianLegalEntity.getRussianBankAccount().getBankBik());
            } else if (contractor.getLegalEntity().isSetInternationalLegalEntity()) {
                InternationalLegalEntity internationalLegalEntity = contractor.getLegalEntity().getInternationalLegalEntity();
                party.setInternationalLegalEntityName(internationalLegalEntity.getLegalName());
                party.setInternationalLegalEntityTradingName(internationalLegalEntity.getTradingName());
                party.setInternationalLegalEntityRegisteredAddress(internationalLegalEntity.getRegisteredAddress());
                party.setInternationalActualAddress(internationalLegalEntity.getActualAddress());
                party.setInternationalLegalEntityRegisteredNumber(internationalLegalEntity.getRegisteredNumber());
            }
        } else if (contractor.isSetPrivateEntity()) {
            party.setPrivateEntityType(TBaseUtil.unionFieldToEnum(contractor.getPrivateEntity(), com.rbkmoney.analytics.domain.db.enums.PrivateEntity.class));
            if (contractor.getPrivateEntity().isSetRussianPrivateEntity()) {
                RussianPrivateEntity russianPrivateEntity = contractor.getPrivateEntity().getRussianPrivateEntity();
                if (russianPrivateEntity.getContactInfo() != null) {
                    party.setRussianPrivateEntityEmail(russianPrivateEntity.getContactInfo().getEmail());
                    party.setRussianPrivateEntityPhoneNumber(russianPrivateEntity.getContactInfo().getPhoneNumber());
                }
                party.setRussianPrivateEntityFirstName(russianPrivateEntity.getFirstName());
                party.setRussianPrivateEntitySecondName(russianPrivateEntity.getSecondName());
                party.setRussianPrivateEntityMiddleName(russianPrivateEntity.getMiddleName());
            }
        }

        return party;
    }

}
