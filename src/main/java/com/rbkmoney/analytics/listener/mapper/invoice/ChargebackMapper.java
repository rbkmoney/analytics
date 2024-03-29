package com.rbkmoney.analytics.listener.mapper.invoice;

import com.rbkmoney.analytics.constant.ChargebackStatus;
import com.rbkmoney.analytics.constant.EventType;
import com.rbkmoney.analytics.dao.model.ChargebackRow;
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
public class ChargebackMapper implements Mapper<InvoiceChange, MachineEvent, ChargebackRow> {

    private final HgClientService hgClientService;
    private final RowFactory<ChargebackRow> chargebackRowRowFactory;

    @Override
    public boolean accept(InvoiceChange change) {
        return getChangeType().getFilter().match(change)
                && (change.getInvoicePaymentChange()
                .getPayload().getInvoicePaymentChargebackChange().getPayload()
                .getInvoicePaymentChargebackStatusChanged().getStatus().isSetAccepted()
                || change.getInvoicePaymentChange().getPayload().getInvoicePaymentChargebackChange()
                .getPayload().getInvoicePaymentChargebackStatusChanged().getStatus().isSetCancelled()
                || change.getInvoicePaymentChange().getPayload().getInvoicePaymentChargebackChange()
                .getPayload().getInvoicePaymentChargebackStatusChanged().getStatus().isSetRejected());
    }

    @Override
    public ChargebackRow map(InvoiceChange change, MachineEvent event) {
        InvoicePaymentChange invoicePaymentChange = change.getInvoicePaymentChange();
        String paymentId = invoicePaymentChange.getId();
        InvoicePaymentChargebackChange invoicePaymentChargebackChange =
                invoicePaymentChange.getPayload().getInvoicePaymentChargebackChange();
        InvoicePaymentChargebackChangePayload payload = invoicePaymentChargebackChange.getPayload();
        InvoicePaymentChargebackStatusChanged invoicePaymentChargebackStatusChanged =
                payload.getInvoicePaymentChargebackStatusChanged();

        String chargebackId = invoicePaymentChargebackChange.getId();
        InvoicePaymentWrapper invoicePaymentWrapper = hgClientService.getInvoiceInfo(event.getSourceId(), findPayment(),
                paymentId, chargebackId, event.getEventId());
        ChargebackRow chargebackRow = chargebackRowRowFactory.create(event, invoicePaymentWrapper, chargebackId);

        chargebackRow.setStatus(TBaseUtil.unionFieldToEnum(
                invoicePaymentChargebackStatusChanged.getStatus(), ChargebackStatus.class));

        log.debug("ChargebackPaymentMapper chargebackRow: {}", chargebackRow);
        return chargebackRow;
    }

    private BiFunction<String, Invoice, Optional<InvoicePayment>> findPayment() {
        return (id, invoiceInfo) -> invoiceInfo.getPayments().stream()
                .filter(payment -> payment.isSetPayment()
                        && payment.isSetChargebacks()
                        && payment.getChargebacks().stream()
                        .anyMatch(chargeback -> chargeback.getChargeback().getId().equals(id))
                )
                .findFirst();
    }

    @Override
    public EventType getChangeType() {
        return EventType.INVOICE_PAYMENT_CHARGEBACK_STATUS_CHANGED;
    }

}
