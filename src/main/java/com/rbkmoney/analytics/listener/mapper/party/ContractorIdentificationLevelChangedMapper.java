package com.rbkmoney.analytics.listener.mapper.party;

import com.rbkmoney.analytics.dao.model.ContractorRow;
import com.rbkmoney.damsel.domain.ContractorIdentificationLevel;
import com.rbkmoney.damsel.payment_processing.ClaimEffect;
import com.rbkmoney.damsel.payment_processing.ContractorEffectUnit;
import com.rbkmoney.damsel.payment_processing.PartyChange;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;

@Slf4j
@Component
public class ContractorIdentificationLevelChangedMapper extends AbstractClaimChangeMapper<ContractorRow> {

    @Override
    public ContractorRow map(PartyChange change, MachineEvent event) {
        List<ClaimEffect> claimEffects = getClaimStatus(change).getAccepted().getEffects();
        ClaimEffect contractorEffect = claimEffects.stream()
                .filter(claimEffect -> claimEffect.isSetContractorEffect() && claimEffect.getContractorEffect().getEffect().isSetIdentificationLevelChanged())
                .findFirst().orElse(null);
        if (contractorEffect != null) {
            return mapEvent(event, contractorEffect);
        }
        return null;
    }

    private ContractorRow mapEvent(MachineEvent event, ClaimEffect effect) {
        ContractorEffectUnit contractorEffect = effect.getContractorEffect();
        ContractorIdentificationLevel identificationLevelChanged = contractorEffect.getEffect().getIdentificationLevelChanged();
        String contractorId = contractorEffect.getId();
        String partyId = event.getSourceId();
        com.rbkmoney.analytics.constant.ContractorIdentificationLevel contractorIdentificationLevel =
                com.rbkmoney.analytics.constant.ContractorIdentificationLevel.valueOf(identificationLevelChanged.name());
        LocalDateTime eventCreatedAt = TypeUtil.stringToLocalDateTime(event.getCreatedAt());

        ContractorRow contractorRow = new ContractorRow();
        contractorRow.setPartyId(partyId);
        contractorRow.setContractorId(contractorId);
        contractorRow.setContractorIdentificationLevel(contractorIdentificationLevel);
        contractorRow.setEventTime(eventCreatedAt);

        return contractorRow;
    }

}
