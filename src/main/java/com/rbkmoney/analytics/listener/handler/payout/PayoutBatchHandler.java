package com.rbkmoney.analytics.listener.handler.payout;

import com.rbkmoney.analytics.dao.model.PayoutRow;
import com.rbkmoney.analytics.dao.repository.MgRepositoryFacade;
import com.rbkmoney.analytics.listener.Processor;
import com.rbkmoney.analytics.listener.handler.BatchHandler;
import com.rbkmoney.analytics.listener.mapper.PayoutMapper;
import com.rbkmoney.damsel.payout_processing.Event;
import com.rbkmoney.damsel.payout_processing.PayoutChange;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.stream.Collectors.toList;

@Component
@RequiredArgsConstructor
public class PayoutBatchHandler implements BatchHandler<PayoutChange, Event> {

    private final MgRepositoryFacade mgRepositoryFacade;
    private final List<PayoutMapper> mappers;

    @Override
    @SuppressWarnings("unchecked")
    public List<PayoutMapper> getMappers() {
        return mappers;
    }

    @Override
    public Processor handle(List<Map.Entry<Event, PayoutChange>> changes) {
        List<PayoutRow> payoutRows = changes.stream()
                .map(changeWithParent -> {
                    PayoutChange change = changeWithParent.getValue();
                    for (PayoutMapper payoutMapper : getMappers()) {
                        if (payoutMapper.accept(change)) {
                            return payoutMapper.map(change, changeWithParent.getKey());
                        }
                    }
                    return null;
                })
                .filter(Objects::nonNull)
                .collect(toList());

        return () -> mgRepositoryFacade.insertPayouts(payoutRows);
    }
}