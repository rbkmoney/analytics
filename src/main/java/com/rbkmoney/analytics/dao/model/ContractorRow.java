package com.rbkmoney.analytics.dao.model;

import com.rbkmoney.analytics.constant.ContractorIdentificationLevel;
import com.rbkmoney.analytics.constant.ContractorType;
import com.rbkmoney.analytics.constant.LegalEntityType;
import com.rbkmoney.analytics.constant.PrivateEntityType;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.time.LocalDateTime;

@Data
@NoArgsConstructor
public class ContractorRow {

    private LocalDateTime eventTime;

    private String partyId;
    private String contractorId;
    private ContractorType contractorType;
    private String regUserEmail;

    private LegalEntityType legalEntityType;
    private String russianLegalEntityName;
    private String russianLegalEntityRegisteredNumber;
    private String russianLegalEntityInn;
    private String russianLegalEntityActualAddress;
    private String russianLegalEntityPostAddress;
    private String russianLegalEntityRepresentativePosition;
    private String russianLegalEntityRepresentativeFullName;
    private String russianLegalEntityRepresentativeDocument;
    private String russianLegalEntityBankAccount;
    private String russianLegalEntityBankName;
    private String russianLegalEntityBankPostAccount;
    private String russianLegalEntityBankBik;

    private String internationalLegalEntityName;
    private String internationalLegalEntityTradingName;
    private String internationalLegalEntityRegisteredAddress;
    private String internationalLegalEntityRegisteredNumber;

    private PrivateEntityType privateEntityType;
    private String russianPrivateEntityFirstName;
    private String russianPrivateEntitySecondName;
    private String russianPrivateEntityMiddleName;
    private String russianPrivateEntityPhoneNumber;
    private String russianPrivateEntityEmail;

    private ContractorIdentificationLevel contractorIdentificationLevel;

}
