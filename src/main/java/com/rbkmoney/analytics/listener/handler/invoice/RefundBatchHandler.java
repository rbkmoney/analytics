package com.rbkmoney.analytics.listener.handler.invoice;

import com.rbkmoney.analytics.dao.model.MgRefundRow;
import com.rbkmoney.analytics.dao.repository.MgRepositoryFacade;
import com.rbkmoney.analytics.listener.Processor;
import com.rbkmoney.analytics.listener.handler.BatchHandler;
import com.rbkmoney.analytics.listener.mapper.invoice.RefundMapper;
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
public class RefundBatchHandler implements BatchHandler<InvoiceChange, MachineEvent> {

    private final MgRepositoryFacade mgRepositoryFacade;
    private final List<RefundMapper> mappers;

    @Override
    @SuppressWarnings("unchecked")
    public List<RefundMapper> getMappers() {
        return mappers;
    }

    @Override
    public Processor handle(List<Map.Entry<MachineEvent, InvoiceChange>> changes) {
        List<MgRefundRow> invoiceEvents = changes.stream()
                .map(changeWithParent -> {
                    InvoiceChange change = changeWithParent.getValue();
                    for (RefundMapper invoiceMapper : getMappers()) {
                        if (invoiceMapper.accept(change)) {
                            return invoiceMapper.map(change, changeWithParent.getKey());
                        }
                    }
                    return null;
                })
                .filter(Objects::nonNull)
                .collect(toList());

        return () -> mgRepositoryFacade.insertRefunds(invoiceEvents);
    }
}
