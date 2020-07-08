package com.rbkmoney.analytics.listener.mapper.party;

import com.rbkmoney.analytics.constant.ContractorType;
import com.rbkmoney.analytics.constant.LegalEntityType;
import com.rbkmoney.analytics.constant.PrivateEntityType;
import com.rbkmoney.analytics.dao.model.ContractorRow;
import com.rbkmoney.damsel.domain.*;
import com.rbkmoney.damsel.payment_processing.ClaimEffect;
import com.rbkmoney.damsel.payment_processing.ContractorEffectUnit;
import com.rbkmoney.damsel.payment_processing.PartyChange;
import com.rbkmoney.geck.common.util.TBaseUtil;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;

@Slf4j
@Component
public class ContractorCreatedMapper extends AbstractClaimChangeMapper<ContractorRow> {

    @Override
    public ContractorRow map(PartyChange change, MachineEvent event) {
        List<ClaimEffect> claimEffects = getClaimStatus(change).getAccepted().getEffects();
        ClaimEffect contractorEffect = claimEffects.stream()
                .filter(claimEffect -> claimEffect.isSetContractorEffect() && claimEffect.getContractorEffect().getEffect().isSetCreated())
                .findFirst().orElse(null);
        if (contractorEffect != null) {
            return mapEvent(event, contractorEffect);
        }
        return null;
    }

    private ContractorRow mapEvent(MachineEvent event, ClaimEffect effect) {
        ContractorEffectUnit contractorEffect = effect.getContractorEffect();
        PartyContractor partyContractor = contractorEffect.getEffect().getCreated();
        Contractor contractor = partyContractor.getContractor();
        com.rbkmoney.damsel.domain.Contractor contractorCreated = partyContractor.getContractor();

        String contractorId = contractorEffect.getId();
        String partyId = event.getSourceId();
        LocalDateTime eventCreatedAt = TypeUtil.stringToLocalDateTime(event.getCreatedAt());

        ContractorRow contractorRow = new ContractorRow();
        contractorRow.setEventTime(eventCreatedAt);
        contractorRow.setPartyId(partyId);
        contractorRow.setContractorId(contractorId);
        contractorRow.setContractorType(TBaseUtil.unionFieldToEnum(contractor, ContractorType.class));
        if (contractor.isSetRegisteredUser()) {
            contractorRow.setRegUserEmail(partyContractor.getContractor().getRegisteredUser().getEmail());
        } else if (contractor.isSetLegalEntity()) {
            contractorRow.setLegalEntityType(TBaseUtil.unionFieldToEnum(contractor.getLegalEntity(), LegalEntityType.class));
            if (contractor.getLegalEntity().isSetRussianLegalEntity()) {
                RussianLegalEntity russianLegalEntity = contractor.getLegalEntity().getRussianLegalEntity();
                contractorRow.setRussianLegalEntityName(russianLegalEntity.getRegisteredName());
                contractorRow.setRussianLegalEntityRegisteredNumber(russianLegalEntity.getRegisteredNumber());
                contractorRow.setRussianLegalEntityInn(russianLegalEntity.getInn());
                contractorRow.setRussianLegalEntityActualAddress(russianLegalEntity.getActualAddress());
                contractorRow.setRussianLegalEntityPostAddress(russianLegalEntity.getPostAddress());
                contractorRow.setRussianLegalEntityRepresentativePosition(russianLegalEntity.getRepresentativePosition());
                contractorRow.setRussianLegalEntityRepresentativeFullName(russianLegalEntity.getRepresentativeFullName());
                contractorRow.setRussianLegalEntityRepresentativeDocument(russianLegalEntity.getRepresentativeDocument());
                contractorRow.setRussianLegalEntityBankAccount(russianLegalEntity.getRussianBankAccount().getAccount());
                contractorRow.setRussianLegalEntityBankName(russianLegalEntity.getRussianBankAccount().getBankName());
                contractorRow.setRussianLegalEntityBankPostAccount(russianLegalEntity.getRussianBankAccount().getBankPostAccount());
                contractorRow.setRussianLegalEntityBankBik(russianLegalEntity.getRussianBankAccount().getBankBik());
            } else if (contractor.getLegalEntity().isSetInternationalLegalEntity()) {
                InternationalLegalEntity internationalLegalEntity = contractor.getLegalEntity().getInternationalLegalEntity();
                contractorRow.setInternationalLegalEntityName(internationalLegalEntity.getLegalName());
                contractorRow.setInternationalLegalEntityTradingName(internationalLegalEntity.getTradingName());
                contractorRow.setInternationalLegalEntityRegisteredAddress(internationalLegalEntity.getRegisteredAddress());
                contractorRow.setRussianLegalEntityActualAddress(internationalLegalEntity.getActualAddress());
                contractorRow.setInternationalLegalEntityRegisteredNumber(internationalLegalEntity.getRegisteredNumber());
            }
        } else if (contractor.isSetPrivateEntity()) {
            contractorRow.setPrivateEntityType(TBaseUtil.unionFieldToEnum(contractor.getPrivateEntity(), PrivateEntityType.class));
            if (contractor.getPrivateEntity().isSetRussianPrivateEntity()) {
                RussianPrivateEntity russianPrivateEntity = contractor.getPrivateEntity().getRussianPrivateEntity();
                if (russianPrivateEntity.getContactInfo() != null) {
                    contractorRow.setRussianPrivateEntityEmail(russianPrivateEntity.getContactInfo().getEmail());
                    contractorRow.setRussianPrivateEntityPhoneNumber(russianPrivateEntity.getContactInfo().getPhoneNumber());
                }
                contractorRow.setRussianPrivateEntityFirstName(russianPrivateEntity.getFirstName());
                contractorRow.setRussianPrivateEntitySecondName(russianPrivateEntity.getSecondName());
                contractorRow.setRussianPrivateEntityMiddleName(russianPrivateEntity.getMiddleName());
            }
        }

        return contractorRow;
    }
}
