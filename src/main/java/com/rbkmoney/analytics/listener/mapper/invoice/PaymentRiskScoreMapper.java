package com.rbkmoney.analytics.listener.mapper.invoice;

import com.rbkmoney.analytics.constant.EventType;
import com.rbkmoney.analytics.dao.model.PaymentRow;
import com.rbkmoney.analytics.domain.InvoicePaymentWrapper;
import com.rbkmoney.analytics.listener.mapper.Mapper;
import com.rbkmoney.analytics.listener.mapper.factory.RowFactory;
import com.rbkmoney.analytics.service.HgClientService;
import com.rbkmoney.damsel.domain.RiskScore;
import com.rbkmoney.damsel.payment_processing.*;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Optional;
import java.util.function.BiFunction;

@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentRiskScoreMapper implements Mapper<InvoiceChange, MachineEvent, PaymentRow> {

    private final HgClientService hgClientService;
    private final RowFactory<PaymentRow> paymentSinkRowFactory;

    @Override
    public PaymentRow map(InvoiceChange change, MachineEvent event) {
        String paymentId = change.getInvoicePaymentChange().getId();
        InvoicePaymentChange invoicePaymentChange = change.getInvoicePaymentChange();
        InvoicePaymentRiskScoreChanged invoicePaymentRiskScoreChanged = invoicePaymentChange.getPayload().getInvoicePaymentRiskScoreChanged();
        RiskScore riskScore = invoicePaymentRiskScoreChanged.getRiskScore();

        BiFunction<String, Invoice, Optional<InvoicePayment>> findPaymentFunc = (id, invoiceInfo) -> invoiceInfo.getPayments().stream()
                .filter(payment -> payment.isSetPayment() && payment.getPayment().getId().equals(id))
                .findFirst();
        InvoicePaymentWrapper invoicePaymentWrapper = hgClientService.getInvoiceInfo(
                event.getSourceId(),
                findPaymentFunc,
                paymentId, event.getEventId());
        PaymentRow paymentRow = paymentSinkRowFactory.create(event, invoicePaymentWrapper, paymentId);

        paymentRow.setRiskScore(riskScore.name());

        log.debug("PaymentRiskScoreMapper paymentRow: {}", paymentRow);

        return paymentRow;
    }

    @Override
    public EventType getChangeType() {
        return EventType.INVOICE_PAYMENT_RISK_SCORE_CHANGED;
    }

}
