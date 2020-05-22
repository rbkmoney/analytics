package com.rbkmoney.analytics.listener.handler;

import com.rbkmoney.analytics.dao.model.MgChargebackRow;
import com.rbkmoney.analytics.dao.repository.MgRepositoryFacade;
import com.rbkmoney.analytics.listener.Processor;
import com.rbkmoney.analytics.listener.mapper.ChargebackPaymentMapper;
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
public class ChargebackBatchHandler implements BatchHandler<InvoiceChange, MachineEvent> {

    private final MgRepositoryFacade mgRepositoryFacade;
    private final List<ChargebackPaymentMapper> mappers;

    @Override
    public List<ChargebackPaymentMapper> getMappers() {
        return mappers;
    }

    @Override
    public Processor handle(List<Map.Entry<MachineEvent, InvoiceChange>> changes) {
        List<MgChargebackRow> invoiceEvents = changes.stream()
                .map(changeWithParent -> {
                    InvoiceChange change = changeWithParent.getValue();
                    for (ChargebackPaymentMapper invoiceMapper : getMappers()) {
                        if (invoiceMapper.accept(change)) {
                            return invoiceMapper.map(change, changeWithParent.getKey());
                        }
                    }
                    return null;
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        return () -> mgRepositoryFacade.insertChargebacks(invoiceEvents);
    }
}
