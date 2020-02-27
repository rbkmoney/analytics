package com.rbkmoney.analytics.listener.handler;

import com.rbkmoney.analytics.dao.model.MgAdjustmentRow;
import com.rbkmoney.analytics.dao.repository.MgAdjustmentRepository;
import com.rbkmoney.analytics.listener.Processor;
import com.rbkmoney.analytics.listener.mapper.AdjustmentPaymentMapper;
import com.rbkmoney.damsel.payment_processing.InvoiceChange;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

@Component
@RequiredArgsConstructor
public class AdjustmentBatchHandler implements BatchHandler<InvoiceChange, MachineEvent> {

    private final MgAdjustmentRepository mgAdjustmentRepository;
    private final List<AdjustmentPaymentMapper> mappers;

    @Override
    public List<AdjustmentPaymentMapper> getMappers() {
        return mappers;
    }

    @Override
    public Processor handle(List<Map.Entry<MachineEvent, InvoiceChange>> changes) {
        List<MgAdjustmentRow> invoiceEvents = changes.stream()
                .map(changeWithParent -> {
                    InvoiceChange change = changeWithParent.getValue();
                    for (AdjustmentPaymentMapper mapper : getMappers()) {
                        if (mapper.accept(change)) {
                            return mapper.map(change, changeWithParent.getKey());
                        }
                    }
                    return null;
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        return () -> mgAdjustmentRepository.insertBatch(invoiceEvents);
    }
}
