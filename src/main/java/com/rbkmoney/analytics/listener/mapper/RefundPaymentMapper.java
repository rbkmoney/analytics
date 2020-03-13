package com.rbkmoney.analytics.listener.mapper;

import com.rbkmoney.analytics.constant.EventType;
import com.rbkmoney.analytics.constant.RefundStatus;
import com.rbkmoney.analytics.dao.model.MgRefundRow;
import com.rbkmoney.analytics.domain.InvoicePaymentWrapper;
import com.rbkmoney.analytics.listener.mapper.factory.RowFactory;
import com.rbkmoney.analytics.service.HgClientService;
import com.rbkmoney.damsel.domain.Failure;
import com.rbkmoney.damsel.payment_processing.*;
import com.rbkmoney.geck.common.util.TBaseUtil;
import com.rbkmoney.geck.serializer.kit.tbase.TErrorUtil;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Optional;
import java.util.function.BiFunction;

@Slf4j
@Component
@RequiredArgsConstructor
public class RefundPaymentMapper implements Mapper<InvoiceChange, MachineEvent, MgRefundRow> {

    public static final String OPERATION_TIMEOUT = "operation_timeout";

    private final HgClientService hgClientService;
    private final RowFactory<MgRefundRow> mgRefundRowFactory;

    @Override
    public MgRefundRow map(InvoiceChange change, MachineEvent event) {
        InvoicePaymentChange invoicePaymentChange = change.getInvoicePaymentChange();
        String paymentId = invoicePaymentChange.getId();
        InvoicePaymentRefundChange invoicePaymentRefundChange = invoicePaymentChange.getPayload().getInvoicePaymentRefundChange();
        InvoicePaymentRefundChangePayload payload = invoicePaymentRefundChange.getPayload();
        InvoicePaymentRefundStatusChanged invoicePaymentRefundStatusChanged = payload.getInvoicePaymentRefundStatusChanged();
        String refundId = invoicePaymentRefundChange.getId();

        InvoicePaymentWrapper invoicePaymentWrapper = hgClientService.getInvoiceInfo(
                event.getSourceId(), findPayment(), paymentId, refundId, event.getEventId());
        MgRefundRow refundRow = mgRefundRowFactory.create(event, invoicePaymentWrapper, refundId);

        refundRow.setStatus(TBaseUtil.unionFieldToEnum(payload
                .getInvoicePaymentRefundStatusChanged()
                .getStatus(), RefundStatus.class));

        if (invoicePaymentRefundStatusChanged.getStatus().isSetFailed()) {
            if (invoicePaymentRefundStatusChanged.getStatus().getFailed().getFailure().isSetFailure()) {
                Failure failure = invoicePaymentRefundStatusChanged.getStatus().getFailed().getFailure().getFailure();
                refundRow.setErrorCode(TErrorUtil.toStringVal(failure));
                refundRow.setErrorReason(failure.getReason());
            } else if (invoicePaymentRefundStatusChanged.getStatus().getFailed().getFailure().isSetOperationTimeout()) {
                refundRow.setErrorCode(OPERATION_TIMEOUT);
            }
        }

        log.debug("RefundPaymentMapper refundRow: {}", refundRow);
        return refundRow;
    }

    private BiFunction<String, Invoice, Optional<InvoicePayment>> findPayment() {
        return (id, invoiceInfo) -> invoiceInfo.getPayments().stream()
                .filter(payment ->
                        payment.isSetPayment()
                                && payment.isSetRefunds()
                                && payment.getRefunds().stream()
                                .anyMatch(refund -> refund.getRefund().getId().equals(id))
                )
                .findFirst();
    }

    @Override
    public EventType getChangeType() {
        return EventType.INVOICE_PAYMENT_REFUND_STATUS_CHANGED;
    }

}
