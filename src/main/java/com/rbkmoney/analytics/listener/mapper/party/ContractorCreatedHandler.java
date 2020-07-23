package com.rbkmoney.analytics.listener.mapper.party;

import com.rbkmoney.analytics.domain.db.enums.ContractorIdentificationLvl;
import com.rbkmoney.analytics.domain.db.tables.pojos.Party;
import com.rbkmoney.damsel.domain.*;
import com.rbkmoney.damsel.payment_processing.ClaimEffect;
import com.rbkmoney.damsel.payment_processing.ContractorEffectUnit;
import com.rbkmoney.damsel.payment_processing.PartyChange;
import com.rbkmoney.geck.common.util.TBaseUtil;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class ContractorCreatedHandler extends AbstractClaimChangeHandler<List<Party>> {

    @Override
    public boolean accept(PartyChange change) {
        return isClaimEffect(change, claimEffect -> claimEffect.isSetContractorEffect()
                && claimEffect.getContractorEffect().getEffect().isSetCreated());
    }

    @Override
    public List<Party> handleChange(PartyChange change, MachineEvent event) {
        List<ClaimEffect> claimEffects = getClaimStatus(change).getAccepted().getEffects();
        List<Party> partyList = new ArrayList<>();
        for (ClaimEffect claimEffect : claimEffects) {
            if (claimEffect.isSetContractorEffect() && claimEffect.getContractorEffect().getEffect().isSetCreated()) {
                partyList.add(handleEvent(event, claimEffect));
            }
        }
        return partyList;
    }

    private Party handleEvent(MachineEvent event, ClaimEffect effect) {
        ContractorEffectUnit contractorEffect = effect.getContractorEffect();
        PartyContractor partyContractor = contractorEffect.getEffect().getCreated();
        Contractor contractor = partyContractor.getContractor();

        log.debug("ContractorCreatedHandler contractor: {}", contractor);

        String contractorId = contractorEffect.getId();
        String partyId = event.getSourceId();

        Party party = new Party();
        party.setPartyId(partyId);
        party.setEventId(event.getEventId());
        party.setEventTime(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));
        party.setContractorId(contractorId);
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
        party.setContractorIdentificationLevel(ContractorIdentificationLvl.valueOf(partyContractor.getStatus().name()));

        log.debug("ContractorCreatedHandler result party: {}", party);

        return party;
    }
}
