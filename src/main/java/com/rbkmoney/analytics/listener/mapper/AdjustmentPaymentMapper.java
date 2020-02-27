package com.rbkmoney.analytics.listener.mapper;

import com.rbkmoney.analytics.constant.AdjustmentStatus;
import com.rbkmoney.analytics.constant.EventType;
import com.rbkmoney.analytics.dao.model.MgAdjustmentRow;
import com.rbkmoney.analytics.exception.RefundInfoNotFoundException;
import com.rbkmoney.analytics.listener.mapper.factory.RowFactory;
import com.rbkmoney.analytics.service.HgClientService;
import com.rbkmoney.damsel.payment_processing.InvoiceChange;
import com.rbkmoney.damsel.payment_processing.InvoicePaymentAdjustmentChange;
import com.rbkmoney.damsel.payment_processing.InvoicePaymentAdjustmentChangePayload;
import com.rbkmoney.geck.common.util.TBaseUtil;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class AdjustmentPaymentMapper implements Mapper<InvoiceChange, MachineEvent, MgAdjustmentRow> {

    private final HgClientService hgClientService;
    private final RowFactory<MgAdjustmentRow> mgAdjustmentRowFactory;

    @Override
    public boolean accept(InvoiceChange change) {
        return getChangeType().getFilter().match(change)
                && change.getInvoicePaymentChange().getPayload().getInvoicePaymentAdjustmentChange().getPayload().
                getInvoicePaymentAdjustmentStatusChanged().getStatus().isSetCaptured();
    }

    @Override
    public MgAdjustmentRow map(InvoiceChange change, MachineEvent event) {
        com.rbkmoney.damsel.payment_processing.Invoice invoiceInfo = hgClientService.getInvoiceInfo(event);
        if (invoiceInfo == null) {
            throw new RefundInfoNotFoundException("Not found refund info in hg!");
        }

        InvoicePaymentAdjustmentChange invoicePaymentAdjustmentChange = change.getInvoicePaymentChange()
                .getPayload()
                .getInvoicePaymentAdjustmentChange();

        InvoicePaymentAdjustmentChangePayload payload = invoicePaymentAdjustmentChange.getPayload();
        String adjustmentChangeId = invoicePaymentAdjustmentChange.getId();
        MgAdjustmentRow mgAdjustmentRow = mgAdjustmentRowFactory.create(event, invoiceInfo, adjustmentChangeId);

        mgAdjustmentRow.setStatus(TBaseUtil.unionFieldToEnum(payload
                .getInvoicePaymentAdjustmentStatusChanged()
                .getStatus(), AdjustmentStatus.class));

        log.debug("RefundPaymentMapper mgAdjustmentRow: {}", mgAdjustmentRow);
        return mgAdjustmentRow;
    }

    @Override
    public EventType getChangeType() {
        return EventType.INVOICE_PAYMENT_ADJUSTMENT_STATUS_CHANGED;
    }

}
