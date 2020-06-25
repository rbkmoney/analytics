package com.rbkmoney.analytics.listener.mapper.factory;

import com.rbkmoney.analytics.computer.CashFlowComputer;
import com.rbkmoney.analytics.dao.model.PaymentRow;
import com.rbkmoney.analytics.domain.InvoicePaymentWrapper;
import com.rbkmoney.analytics.service.GeoProvider;
import com.rbkmoney.damsel.domain.FinalCashFlowPosting;
import com.rbkmoney.damsel.payment_processing.InvoicePayment;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;

@Slf4j
@Service
public class PaymentRowFactory extends InvoiceBaseRowFactory<PaymentRow> {

    private final CashFlowComputer cashFlowComputer;

    public PaymentRowFactory(GeoProvider geoProvider, CashFlowComputer cashFlowComputer) {
        super(geoProvider);
        this.cashFlowComputer = cashFlowComputer;
    }

    @Override
    public PaymentRow create(MachineEvent machineEvent, InvoicePaymentWrapper invoicePaymentWrapper, String paymentId) {
        PaymentRow paymentRow = new PaymentRow();
        InvoicePayment payment = invoicePaymentWrapper.getInvoicePayment();
        List<FinalCashFlowPosting> cashFlow = payment.getCashFlow();
        paymentRow.setCashFlowResult(cashFlowComputer.compute(cashFlow));
        initBaseRow(machineEvent, paymentRow, payment, invoicePaymentWrapper.getInvoice());
        return paymentRow;
    }

}
