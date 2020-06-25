package com.rbkmoney.analytics.listener.mapper.invoice;

import com.rbkmoney.analytics.constant.AdjustmentStatus;
import com.rbkmoney.analytics.constant.EventType;
import com.rbkmoney.analytics.dao.model.AdjustmentRow;
import com.rbkmoney.analytics.domain.InvoicePaymentWrapper;
import com.rbkmoney.analytics.listener.mapper.Mapper;
import com.rbkmoney.analytics.listener.mapper.factory.RowFactory;
import com.rbkmoney.analytics.service.HgClientService;
import com.rbkmoney.damsel.payment_processing.*;
import com.rbkmoney.geck.common.util.TBaseUtil;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Optional;
import java.util.function.BiFunction;

@Slf4j
@Component
@RequiredArgsConstructor
public class AdjustmentMapper implements Mapper<InvoiceChange, MachineEvent, AdjustmentRow> {

    private final HgClientService hgClientService;
    private final RowFactory<AdjustmentRow> adjustmentRowFactory;

    @Override
    public boolean accept(InvoiceChange change) {
        return getChangeType().getFilter().match(change);
    }

    @Override
    public AdjustmentRow map(InvoiceChange change, MachineEvent event) {
        InvoicePaymentChange invoicePaymentChange = change.getInvoicePaymentChange();
        String paymentId = invoicePaymentChange.getId();
        InvoicePaymentAdjustmentChange invoicePaymentAdjustmentChange = invoicePaymentChange
                .getPayload()
                .getInvoicePaymentAdjustmentChange();
        InvoicePaymentAdjustmentChangePayload payload = invoicePaymentAdjustmentChange.getPayload();
        String adjustmentChangeId = invoicePaymentAdjustmentChange.getId();

        InvoicePaymentWrapper invoicePaymentWrapper = hgClientService.getInvoiceInfo(
                event.getSourceId(), findPayment(), paymentId, adjustmentChangeId, event.getEventId());
        AdjustmentRow adjustmentRow = adjustmentRowFactory.create(event, invoicePaymentWrapper, adjustmentChangeId);

        adjustmentRow.setStatus(TBaseUtil.unionFieldToEnum(payload
                .getInvoicePaymentAdjustmentStatusChanged()
                .getStatus(), AdjustmentStatus.class));

        log.debug("AdjustmentMapper adjustmentRow: {}", adjustmentRow);
        return adjustmentRow;
    }

    private BiFunction<String, Invoice, Optional<InvoicePayment>> findPayment() {
        return (id, invoiceInfo) -> invoiceInfo.getPayments().stream()
                .filter(payment ->
                        payment.isSetPayment()
                                && payment.isSetAdjustments()
                                && payment.getAdjustments().stream()
                                .anyMatch(adjustment -> adjustment.getId().equals(id))
                )
                .findFirst();
    }

    @Override
    public EventType getChangeType() {
        return EventType.INVOICE_PAYMENT_ADJUSTMENT_STATUS_CHANGED;
    }

}
