package com.rbkmoney.analytics.listener.mapper;

import com.rbkmoney.analytics.constant.AdjustmentStatus;
import com.rbkmoney.analytics.constant.EventType;
import com.rbkmoney.analytics.dao.model.MgAdjustmentRow;
import com.rbkmoney.analytics.domain.InvoicePaymentWrapper;
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
public class AdjustmentPaymentMapper implements Mapper<InvoiceChange, MachineEvent, MgAdjustmentRow> {

    private final HgClientService hgClientService;
    private final RowFactory<MgAdjustmentRow> mgAdjustmentRowFactory;

    @Override
    public boolean accept(InvoiceChange change) {
        return getChangeType().getFilter().match(change);
    }

    @Override
    public MgAdjustmentRow map(InvoiceChange change, MachineEvent event) {
        InvoicePaymentChange invoicePaymentChange = change.getInvoicePaymentChange();
        String paymentId = invoicePaymentChange.getId();
        InvoicePaymentAdjustmentChange invoicePaymentAdjustmentChange = invoicePaymentChange
                .getPayload()
                .getInvoicePaymentAdjustmentChange();
        InvoicePaymentAdjustmentChangePayload payload = invoicePaymentAdjustmentChange.getPayload();
        String adjustmentChangeId = invoicePaymentAdjustmentChange.getId();

        InvoicePaymentWrapper invoicePaymentWrapper = hgClientService.getInvoiceInfo(
                event.getSourceId(), findPayment(), paymentId, adjustmentChangeId, event.getEventId());
        MgAdjustmentRow mgAdjustmentRow = mgAdjustmentRowFactory.create(event, invoicePaymentWrapper, adjustmentChangeId);

        mgAdjustmentRow.setStatus(TBaseUtil.unionFieldToEnum(payload
                .getInvoicePaymentAdjustmentStatusChanged()
                .getStatus(), AdjustmentStatus.class));

        log.debug("RefundPaymentMapper mgAdjustmentRow: {}", mgAdjustmentRow);
        return mgAdjustmentRow;
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
