package com.rbkmoney.analytics.listener.mapper;

import com.rbkmoney.analytics.constant.EventType;
import com.rbkmoney.analytics.constant.PaymentStatus;
import com.rbkmoney.analytics.dao.model.MgPaymentSinkRow;
import com.rbkmoney.analytics.exception.PaymentInfoNotFoundException;
import com.rbkmoney.analytics.listener.mapper.factory.RowFactory;
import com.rbkmoney.analytics.service.HgClientService;
import com.rbkmoney.damsel.domain.Failure;
import com.rbkmoney.damsel.domain.OperationFailure;
import com.rbkmoney.damsel.payment_processing.InvoiceChange;
import com.rbkmoney.damsel.payment_processing.InvoicePaymentChange;
import com.rbkmoney.damsel.payment_processing.InvoicePaymentChangePayload;
import com.rbkmoney.damsel.payment_processing.InvoicePaymentStatusChanged;
import com.rbkmoney.geck.common.util.TBaseUtil;
import com.rbkmoney.geck.serializer.kit.tbase.TErrorUtil;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class InvoicePaymentMapper implements Mapper<InvoiceChange, MachineEvent, MgPaymentSinkRow> {

    public static final String OPERATION_TIMEOUT = "operation_timeout";

    private final HgClientService hgClientService;
    private final RowFactory<MgPaymentSinkRow> mgPaymentSinkRowFactory;

    @Override
    public boolean accept(InvoiceChange change) {
        return getChangeType().getFilter().match(change)
                && (change.getInvoicePaymentChange().getPayload().getInvoicePaymentStatusChanged().getStatus().isSetFailed()
                || change.getInvoicePaymentChange().getPayload().getInvoicePaymentStatusChanged().getStatus().isSetCancelled()
                || change.getInvoicePaymentChange().getPayload().getInvoicePaymentStatusChanged().getStatus().isSetCaptured());
    }

    @Override
    public MgPaymentSinkRow map(InvoiceChange change, MachineEvent event) {
        log.debug("InvoicePaymentMapper change: {} event: {}", change, event);

        com.rbkmoney.damsel.payment_processing.Invoice invoiceInfo = hgClientService.getInvoiceInfo(event);
        if (invoiceInfo == null) {
            throw new PaymentInfoNotFoundException("Not found payment info in hg!");
        }

        log.debug("InvoicePaymentMapper invoiceInfo: {}", invoiceInfo);

        InvoicePaymentChange invoicePaymentChange = change.getInvoicePaymentChange();
        InvoicePaymentChangePayload payload = invoicePaymentChange.getPayload();
        InvoicePaymentStatusChanged invoicePaymentStatusChanged = payload.getInvoicePaymentStatusChanged();

        String paymentId = change.getInvoicePaymentChange().getId();
        MgPaymentSinkRow mgPaymentSinkRow = mgPaymentSinkRowFactory.create(event, invoiceInfo, paymentId);
        mgPaymentSinkRow.setStatus(TBaseUtil.unionFieldToEnum(invoicePaymentStatusChanged.getStatus(), PaymentStatus.class));

        if (invoicePaymentStatusChanged.getStatus().isSetFailed()) {
            OperationFailure operationFailure = invoicePaymentStatusChanged.getStatus().getFailed().getFailure();
            if (operationFailure.isSetFailure()) {
                Failure failure = operationFailure.getFailure();
                mgPaymentSinkRow.setErrorCode(TErrorUtil.toStringVal(failure));
                mgPaymentSinkRow.setErrorReason(failure.getReason());
            } else if (invoicePaymentStatusChanged.getStatus().getFailed().getFailure().isSetOperationTimeout()) {
                mgPaymentSinkRow.setErrorCode(OPERATION_TIMEOUT);
            }
        }

        log.debug("InvoicePaymentMapper mgPaymentSinkRow: {}", mgPaymentSinkRow);
        return mgPaymentSinkRow;
    }

    @Override
    public EventType getChangeType() {
        return EventType.INVOICE_PAYMENT_STATUS_CHANGED;
    }

}
