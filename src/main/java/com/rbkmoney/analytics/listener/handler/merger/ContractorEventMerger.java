package com.rbkmoney.analytics.listener.handler.merger;

import com.rbkmoney.analytics.domain.db.tables.pojos.Contractor;
import com.rbkmoney.analytics.service.PartyManagementService;
import com.rbkmoney.analytics.service.model.PartyGeneralKey;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@RequiredArgsConstructor
public class ContractorEventMerger {

    private final PartyManagementService partyManagementService;

    public Contractor merge(PartyGeneralKey key, List<Contractor> currentContractors) {
        Contractor targetContractor = partyManagementService.getContractorById(key.getRefId());
        if (targetContractor == null) {
            targetContractor = new Contractor();
        }
        for (Contractor currentContractor : currentContractors) {
            targetContractor.setPartyId(currentContractor.getPartyId() != null ? currentContractor.getPartyId() : targetContractor.getPartyId());
            targetContractor.setEventId(currentContractor.getEventId());
            targetContractor.setEventTime(currentContractor.getEventTime());
            targetContractor.setContractorId(currentContractor.getContractorId() != null ? currentContractor.getContractorId() : targetContractor.getContractorId());
            targetContractor.setContractorType(currentContractor.getContractorType() != null ? currentContractor.getContractorType() : targetContractor.getContractorType());
            targetContractor.setRegUserEmail(currentContractor.getRegUserEmail() != null ? currentContractor.getRegUserEmail() : targetContractor.getRegUserEmail());
            targetContractor.setLegalEntityType(currentContractor.getLegalEntityType() != null ? currentContractor.getLegalEntityType() : targetContractor.getLegalEntityType());
            targetContractor.setRussianLegalEntityInn(currentContractor.getRussianLegalEntityInn() != null ? currentContractor.getRussianLegalEntityInn() : targetContractor.getRussianLegalEntityInn());
            targetContractor.setRussianLegalEntityName(currentContractor.getRussianLegalEntityName() != null ? currentContractor.getRussianLegalEntityName() : targetContractor.getRussianLegalEntityName());
            targetContractor.setRussianLegalEntityRegisteredNumber(currentContractor.getRussianLegalEntityRegisteredNumber() != null ? currentContractor.getRussianLegalEntityRegisteredNumber() : targetContractor.getRussianLegalEntityRegisteredNumber());
            targetContractor.setRussianLegalEntityActualAddress(currentContractor.getRussianLegalEntityActualAddress() != null ? currentContractor.getRussianLegalEntityActualAddress() : targetContractor.getRussianLegalEntityActualAddress());
            targetContractor.setRussianLegalEntityPostAddress(currentContractor.getRussianLegalEntityPostAddress() != null ? currentContractor.getRussianLegalEntityPostAddress() : targetContractor.getRussianLegalEntityPostAddress());
            targetContractor.setRussianLegalEntityRepresentativePosition(currentContractor.getRussianLegalEntityRepresentativePosition() != null ? currentContractor.getRussianLegalEntityRepresentativePosition() : targetContractor.getRussianLegalEntityRepresentativePosition());
            targetContractor.setRussianLegalEntityRepresentativeDocument(currentContractor.getRussianLegalEntityRepresentativeDocument() != null ? currentContractor.getRussianLegalEntityRepresentativeDocument() : targetContractor.getRussianLegalEntityRepresentativeDocument());
            targetContractor.setRussianLegalEntityRepresentativeFullName(currentContractor.getRussianLegalEntityRepresentativeFullName() != null ? currentContractor.getRussianLegalEntityRepresentativeFullName() : targetContractor.getRussianLegalEntityRepresentativeFullName());
            targetContractor.setRussianLegalEntityBankAccount(currentContractor.getRussianLegalEntityBankAccount() != null ? currentContractor.getRussianLegalEntityBankAccount() : targetContractor.getRussianLegalEntityBankAccount());
            targetContractor.setRussianLegalEntityBankBik(currentContractor.getRussianLegalEntityBankBik() != null ? currentContractor.getRussianLegalEntityBankBik() : targetContractor.getRussianLegalEntityBankBik());
            targetContractor.setRussianLegalEntityBankName(currentContractor.getRussianLegalEntityBankName() != null ? currentContractor.getRussianLegalEntityBankName() : targetContractor.getRussianLegalEntityBankName());
            targetContractor.setRussianLegalEntityBankPostAccount(currentContractor.getRussianLegalEntityBankPostAccount() != null ? currentContractor.getRussianLegalEntityBankPostAccount() : targetContractor.getRussianLegalEntityBankPostAccount());
            targetContractor.setInternationalLegalEntityName(currentContractor.getInternationalLegalEntityName() != null ? currentContractor.getInternationalLegalEntityName() : targetContractor.getInternationalLegalEntityName());
            targetContractor.setInternationalLegalEntityRegisteredAddress(currentContractor.getInternationalLegalEntityRegisteredAddress() != null ? currentContractor.getInternationalLegalEntityRegisteredAddress() : targetContractor.getInternationalLegalEntityRegisteredAddress());
            targetContractor.setInternationalLegalEntityRegisteredNumber(currentContractor.getInternationalLegalEntityRegisteredNumber() != null ? currentContractor.getInternationalLegalEntityRegisteredNumber() : targetContractor.getInternationalLegalEntityRegisteredNumber());
            targetContractor.setInternationalLegalEntityTradingName(currentContractor.getInternationalLegalEntityTradingName() != null ? currentContractor.getInternationalLegalEntityTradingName() : targetContractor.getInternationalLegalEntityTradingName());
            targetContractor.setPrivateEntityType(currentContractor.getPrivateEntityType() != null ? currentContractor.getPrivateEntityType() : targetContractor.getPrivateEntityType());
            targetContractor.setRussianPrivateEntityEmail(currentContractor.getRussianPrivateEntityEmail() != null ? currentContractor.getRussianPrivateEntityEmail() : targetContractor.getRussianPrivateEntityEmail());
            targetContractor.setRussianPrivateEntityFirstName(currentContractor.getRussianPrivateEntityFirstName() != null ? currentContractor.getRussianPrivateEntityFirstName() : targetContractor.getRussianPrivateEntityFirstName());
            targetContractor.setRussianPrivateEntitySecondName(currentContractor.getRussianPrivateEntitySecondName() != null ? currentContractor.getRussianPrivateEntitySecondName() : targetContractor.getRussianPrivateEntitySecondName());
            targetContractor.setRussianPrivateEntityMiddleName(currentContractor.getRussianPrivateEntityMiddleName() != null ? currentContractor.getRussianPrivateEntityMiddleName() : targetContractor.getRussianPrivateEntityMiddleName());
            targetContractor.setRussianPrivateEntityPhoneNumber(currentContractor.getRussianPrivateEntityPhoneNumber() != null ? currentContractor.getRussianPrivateEntityPhoneNumber() : targetContractor.getRussianPrivateEntityPhoneNumber());
            targetContractor.setContractorIdentificationLevel(currentContractor.getContractorIdentificationLevel() != null ? currentContractor.getContractorIdentificationLevel() : targetContractor.getContractorIdentificationLevel());
        }

        return targetContractor;
    }
}
