package com.rbkmoney.analytics.listener.handler.invoice;

import com.rbkmoney.analytics.dao.model.MgChargebackRow;
import com.rbkmoney.analytics.dao.repository.MgRepositoryFacade;
import com.rbkmoney.analytics.listener.Processor;
import com.rbkmoney.analytics.listener.handler.BatchHandler;
import com.rbkmoney.analytics.listener.mapper.ChargebackPaymentMapper;
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
public class ChargebackBatchHandler implements BatchHandler<InvoiceChange, MachineEvent> {

    private final MgRepositoryFacade mgRepositoryFacade;
    private final List<ChargebackPaymentMapper> mappers;

    @Override
    @SuppressWarnings("unchecked")
    public List<ChargebackPaymentMapper> getMappers() {
        return mappers;
    }

    @Override
    public Processor handle(List<Map.Entry<MachineEvent, InvoiceChange>> changes) {
        List<MgChargebackRow> invoiceEvents = changes.stream()
                .map(this::findAndMapChange)
                .filter(Objects::nonNull)
                .collect(toList());

        return () -> mgRepositoryFacade.insertChargebacks(invoiceEvents);
    }

    private MgChargebackRow findAndMapChange(Map.Entry<MachineEvent, InvoiceChange> changeWithParent) {
        InvoiceChange change = changeWithParent.getValue();
        for (ChargebackPaymentMapper invoiceMapper : getMappers()) {
            if (invoiceMapper.accept(change)) {
                return invoiceMapper.map(change, changeWithParent.getKey());
            }
        }

        return null;
    }
}
