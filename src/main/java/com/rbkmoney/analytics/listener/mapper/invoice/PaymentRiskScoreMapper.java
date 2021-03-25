package com.rbkmoney.analytics.listener.mapper.invoice;

import com.rbkmoney.analytics.constant.EventType;
import com.rbkmoney.analytics.dao.model.PaymentRow;
import com.rbkmoney.analytics.domain.InvoicePaymentWrapper;
import com.rbkmoney.analytics.listener.mapper.AbstractMapper;
import com.rbkmoney.analytics.listener.mapper.factory.RowFactory;
import com.rbkmoney.analytics.service.HgClientService;
import com.rbkmoney.damsel.domain.RiskScore;
import com.rbkmoney.damsel.payment_processing.InvoiceChange;
import com.rbkmoney.damsel.payment_processing.InvoicePaymentChange;
import com.rbkmoney.damsel.payment_processing.InvoicePaymentRiskScoreChanged;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentRiskScoreMapper extends AbstractMapper<InvoiceChange, MachineEvent, PaymentRow> {

    private final HgClientService hgClientService;
    private final RowFactory<PaymentRow> paymentSinkRowFactory;

    @Override
    public PaymentRow map(InvoiceChange change, MachineEvent event) {
        String paymentId = change.getInvoicePaymentChange().getId();
        InvoicePaymentChange invoicePaymentChange = change.getInvoicePaymentChange();
        InvoicePaymentRiskScoreChanged invoicePaymentRiskScoreChanged = invoicePaymentChange.getPayload()
                .getInvoicePaymentRiskScoreChanged();
        RiskScore riskScore = invoicePaymentRiskScoreChanged.getRiskScore();

        InvoicePaymentWrapper invoicePaymentWrapper = hgClientService.getInvoiceInfo(
                event.getSourceId(),
                findPayment(),
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
