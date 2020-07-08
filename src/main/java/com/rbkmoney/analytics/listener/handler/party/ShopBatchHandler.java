package com.rbkmoney.analytics.listener.handler.party;

import com.rbkmoney.analytics.dao.model.ContractorRow;
import com.rbkmoney.analytics.dao.model.ShopRow;
import com.rbkmoney.analytics.dao.repository.RepositoryFacade;
import com.rbkmoney.analytics.listener.Processor;
import com.rbkmoney.analytics.listener.handler.BatchHandler;
import com.rbkmoney.analytics.listener.mapper.Mapper;
import com.rbkmoney.analytics.listener.mapper.party.AbstractClaimChangeMapper;
import com.rbkmoney.damsel.payment_processing.PartyChange;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.stream.Collectors.toList;

@Component
@RequiredArgsConstructor
public class ShopBatchHandler implements BatchHandler<PartyChange, MachineEvent> {

    private final RepositoryFacade repositoryFacade;

    private final List<Mapper<PartyChange, MachineEvent, ShopRow>> mappers;

    @Override
    public Processor handle(List<Map.Entry<MachineEvent, PartyChange>> changes) {
        List<ShopRow> shopRows = changes.stream()
                .map(changeWithParent -> {
                    PartyChange change = changeWithParent.getValue();
                    for (Mapper<PartyChange, MachineEvent, ShopRow> contractorMapper : getMappers()) {
                        if (contractorMapper.accept(change)) {
                            return contractorMapper.map(change, changeWithParent.getKey());
                        }
                    }
                    return null;
                })
                .filter(Objects::nonNull)
                .collect(toList());

        return () -> repositoryFacade.insertShops(shopRows);
    }

    @Override
    public List<Mapper<PartyChange, MachineEvent, ShopRow>> getMappers() {
        return mappers;
    }
}
