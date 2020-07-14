package com.rbkmoney.analytics.listener.handler.party;

import com.rbkmoney.analytics.dao.repository.RepositoryFacade;
import com.rbkmoney.analytics.domain.db.tables.pojos.Shop;
import com.rbkmoney.analytics.listener.Processor;
import com.rbkmoney.analytics.listener.handler.AdvancedBatchHandler;
import com.rbkmoney.analytics.listener.mapper.AdvancedMapper;
import com.rbkmoney.analytics.listener.mapper.LocalStorage;
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
public class ShopBatchHandler implements AdvancedBatchHandler<PartyChange, MachineEvent> {

    private final RepositoryFacade repositoryFacade;

    private final List<AdvancedMapper<PartyChange, MachineEvent, Shop>> mappers;

    @Override
    public Processor handle(List<Map.Entry<MachineEvent, PartyChange>> changes) {
        LocalStorage<Shop> storage = new LocalStorage<>();
        List<Shop> shops = changes.stream()
                .map(changeWithParent -> {
                    PartyChange change = changeWithParent.getValue();
                    for (AdvancedMapper<PartyChange, MachineEvent, Shop> shopMapper : getMappers()) {
                        if (shopMapper.accept(change)) {
                            Shop shop = shopMapper.map(change, changeWithParent.getKey(), storage);
                            if (shop != null) {
                                storage.put(shop.getPartyId() + shop.getShopId(), shop);
                            }
                            return shop;
                        }
                    }
                    return null;
                })
                .filter(Objects::nonNull)
                .collect(toList());

        return () -> repositoryFacade.insertShops(shops);
    }

    @Override
    public List<AdvancedMapper<PartyChange, MachineEvent, Shop>> getMappers() {
        return mappers;
    }
}
