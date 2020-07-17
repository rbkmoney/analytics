package com.rbkmoney.analytics.listener.handler.party;

import com.rbkmoney.analytics.dao.repository.RepositoryFacade;
import com.rbkmoney.analytics.domain.db.tables.pojos.Shop;
import com.rbkmoney.analytics.listener.Processor;
import com.rbkmoney.analytics.listener.handler.AdvancedBatchHandler;
import com.rbkmoney.analytics.listener.mapper.ChangeHandler;
import com.rbkmoney.damsel.payment_processing.PartyChange;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Component
@RequiredArgsConstructor
public class ShopBatchHandler implements AdvancedBatchHandler<PartyChange, MachineEvent> {

    @Getter
    private final List<ChangeHandler<PartyChange, MachineEvent, Shop>> handlers;

    @Override
    public Processor handle(List<Map.Entry<MachineEvent, PartyChange>> changes) {
        return () -> {
            for (Map.Entry<MachineEvent, PartyChange> change : changes) {
                handleShopChange(change);
            }
        };
    }

    private void handleShopChange(Map.Entry<MachineEvent, PartyChange> changeEntry) {
        PartyChange change = changeEntry.getValue();
        for (ChangeHandler<PartyChange, MachineEvent, Shop> shopHandler : getHandlers()) {
            if (shopHandler.accept(change)) {
                shopHandler.handleChange(change, changeEntry.getKey());
            }
        }
    }

}
