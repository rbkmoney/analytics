package com.rbkmoney.analytics.listener.handler.invoice;

import com.rbkmoney.analytics.dao.model.MgAdjustmentRow;
import com.rbkmoney.analytics.dao.repository.MgRepositoryFacade;
import com.rbkmoney.analytics.listener.Processor;
import com.rbkmoney.analytics.listener.handler.BatchHandler;
import com.rbkmoney.analytics.listener.mapper.invoice.AdjustmentMapper;
import com.rbkmoney.damsel.payment_processing.InvoiceChange;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.stream.Collectors.toList;

@Component
@RequiredArgsConstructor
public class AdjustmentBatchHandler implements BatchHandler<InvoiceChange, MachineEvent> {

    private final MgRepositoryFacade mgRepositoryFacade;
    private final List<AdjustmentMapper> mappers;

    @Override
    @SuppressWarnings("unchecked")
    public List<AdjustmentMapper> getMappers() {
        return mappers;
    }

    @Override
    public Processor handle(List<Map.Entry<MachineEvent, InvoiceChange>> changes) {
        List<MgAdjustmentRow> invoiceEvents = changes.stream()
                .map(changeWithParent -> {
                    InvoiceChange change = changeWithParent.getValue();
                    for (AdjustmentMapper mapper : getMappers()) {
                        if (mapper.accept(change)) {
                            return mapper.map(change, changeWithParent.getKey());
                        }
                    }
                    return null;
                })
                .filter(Objects::nonNull)
                .collect(toList());

        return () -> mgRepositoryFacade.insertAdjustments(invoiceEvents);
    }
}
