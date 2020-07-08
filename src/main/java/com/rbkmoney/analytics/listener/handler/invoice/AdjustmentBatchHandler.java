package com.rbkmoney.analytics.listener.handler.invoice;

import com.rbkmoney.analytics.dao.model.AdjustmentRow;
import com.rbkmoney.analytics.dao.repository.RepositoryFacade;
import com.rbkmoney.analytics.listener.Processor;
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
public class AdjustmentBatchHandler implements InvoiceBatchHandler {

    private final RepositoryFacade repositoryFacade;
    private final List<AdjustmentMapper> mappers;

    @Override
    @SuppressWarnings("unchecked")
    public List<AdjustmentMapper> getMappers() {
        return mappers;
    }

    @Override
    public Processor handle(List<Map.Entry<MachineEvent, InvoiceChange>> changes) {
        List<AdjustmentRow> invoiceEvents = changes.stream()
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

        return () -> repositoryFacade.insertAdjustments(invoiceEvents);
    }
}
