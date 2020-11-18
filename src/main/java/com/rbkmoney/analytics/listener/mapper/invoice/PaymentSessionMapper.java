package com.rbkmoney.analytics.listener.mapper.invoice;

import com.rbkmoney.analytics.constant.EventType;
import com.rbkmoney.analytics.dao.model.PaymentRow;
import com.rbkmoney.analytics.domain.InvoicePaymentWrapper;
import com.rbkmoney.analytics.listener.mapper.Mapper;
import com.rbkmoney.analytics.listener.mapper.factory.RowFactory;
import com.rbkmoney.analytics.service.HgClientService;
import com.rbkmoney.damsel.domain.AdditionalTransactionInfo;
import com.rbkmoney.damsel.domain.TransactionInfo;
import com.rbkmoney.damsel.payment_processing.*;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Optional;
import java.util.function.BiFunction;

@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentSessionMapper implements Mapper<InvoiceChange, MachineEvent, PaymentRow> {

    private final HgClientService hgClientService;
    private final RowFactory<PaymentRow> paymentSinkRowFactory;

    @Override
    public PaymentRow map(InvoiceChange change, MachineEvent event) {
        String paymentId = change.getInvoicePaymentChange().getId();
        InvoicePaymentChange invoicePaymentChange = change.getInvoicePaymentChange();
        InvoicePaymentSessionChange sessionChange = invoicePaymentChange.getPayload().getInvoicePaymentSessionChange();
        SessionChangePayload payload = sessionChange.getPayload();

        BiFunction<String, Invoice, Optional<InvoicePayment>> findPaymentFunc = (id, invoiceInfo) -> invoiceInfo.getPayments().stream()
                .filter(payment -> payment.isSetPayment() && payment.getPayment().getId().equals(id))
                .findFirst();
        InvoicePaymentWrapper invoicePaymentWrapper = hgClientService.getInvoiceInfo(
                event.getSourceId(),
                findPaymentFunc,
                paymentId, event.getEventId());
        PaymentRow paymentRow = paymentSinkRowFactory.create(event, invoicePaymentWrapper, paymentId);

        TransactionInfo transactionInfo = payload.getSessionTransactionBound().getTrx();
        if (transactionInfo.isSetAdditionalInfo()) {
            AdditionalTransactionInfo additionalTransactionInfo = transactionInfo.getAdditionalInfo();
            paymentRow.setRrn(additionalTransactionInfo.getRrn());
        }

        log.debug("PaymentSessionMapper paymentRow: {}", paymentRow);
        return paymentRow;
    }

    @Override
    public EventType getChangeType() {
        return EventType.INVOICE_PAYMENT_SESSION_CHANGED;
    }
}
