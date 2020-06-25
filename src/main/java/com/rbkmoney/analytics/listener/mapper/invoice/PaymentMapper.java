package com.rbkmoney.analytics.listener.mapper.invoice;

import com.rbkmoney.analytics.constant.EventType;
import com.rbkmoney.analytics.constant.PaymentStatus;
import com.rbkmoney.analytics.dao.model.MgPaymentSinkRow;
import com.rbkmoney.analytics.domain.CashFlowResult;
import com.rbkmoney.analytics.domain.InvoicePaymentWrapper;
import com.rbkmoney.analytics.listener.mapper.Mapper;
import com.rbkmoney.analytics.listener.mapper.factory.RowFactory;
import com.rbkmoney.analytics.service.HgClientService;
import com.rbkmoney.damsel.domain.Failure;
import com.rbkmoney.damsel.domain.OperationFailure;
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
public class PaymentMapper implements Mapper<InvoiceChange, MachineEvent, MgPaymentSinkRow> {

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
        String paymentId = change.getInvoicePaymentChange().getId();
        InvoicePaymentChange invoicePaymentChange = change.getInvoicePaymentChange();
        InvoicePaymentChangePayload payload = invoicePaymentChange.getPayload();
        InvoicePaymentStatusChanged invoicePaymentStatusChanged = payload.getInvoicePaymentStatusChanged();

        InvoicePaymentWrapper invoicePaymentWrapper = hgClientService.getInvoiceInfo(event.getSourceId(), findPayment(), paymentId, event.getEventId());

        MgPaymentSinkRow mgPaymentSinkRow = mgPaymentSinkRowFactory.create(event, invoicePaymentWrapper, paymentId);

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
            if (mgPaymentSinkRow.getCashFlowResult() == null || mgPaymentSinkRow.getCashFlowResult().getAmount() == 0) {
                mgPaymentSinkRow.setCashFlowResult(CashFlowResult.builder()
                        .amount(invoicePaymentWrapper.getInvoicePayment().getPayment().getCost().getAmount())
                        .build());
            }
        }

        log.debug("InvoicePaymentMapper mgPaymentSinkRow: {}", mgPaymentSinkRow);
        return mgPaymentSinkRow;
    }

    private BiFunction<String, Invoice, Optional<InvoicePayment>> findPayment() {
        return (id, invoiceInfo) -> invoiceInfo.getPayments().stream()
                .filter(payment -> payment.isSetPayment() && payment.getPayment().getId().equals(id))
                .findFirst();
    }

    @Override
    public EventType getChangeType() {
        return EventType.INVOICE_PAYMENT_STATUS_CHANGED;
    }

}
