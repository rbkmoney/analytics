package com.rbkmoney.analytics.listener.handler.party;

import com.rbkmoney.analytics.domain.db.tables.pojos.Shop;
import com.rbkmoney.analytics.listener.handler.merger.ShopEventMerger;
import com.rbkmoney.analytics.listener.mapper.ChangeHandler;
import com.rbkmoney.analytics.service.PartyManagementService;
import com.rbkmoney.analytics.service.model.GeneralKey;
import com.rbkmoney.damsel.payment_processing.PartyChange;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Component
@RequiredArgsConstructor
public class ShopEventHandler implements PartyManagementEventHandler {

    private final List<ChangeHandler<PartyChange, MachineEvent, List<Shop>>> shopHandlers;
    private final ShopEventMerger shopEventMerger;
    private final PartyManagementService partyManagementService;

    @Override
    public void handle(MachineEvent machineEvent, PartyChange change) {
        final List<Shop> shops = shopHandlers.stream()
                .filter(changeHandler -> changeHandler.accept(change))
                .flatMap(changeHandler -> changeHandler.handleChange(change, machineEvent).stream())
                .collect(Collectors.groupingBy(o -> new GeneralKey(o.getPartyId(), o.getShopId()), Collectors.toList()))
                .entrySet().stream()
                .map(shopKeyListEntry -> shopEventMerger.mergeShop(shopKeyListEntry.getKey(), shopKeyListEntry.getValue()))
                .collect(Collectors.toList());
        if (!shops.isEmpty()) {
            partyManagementService.saveShop(shops);
        }
    }

}
