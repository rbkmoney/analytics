package com.rbkmoney.analytics.listener.mapper.factory;

import com.rbkmoney.analytics.computer.CashFlowComputer;
import com.rbkmoney.analytics.dao.model.RefundRow;
import com.rbkmoney.analytics.domain.InvoicePaymentWrapper;
import com.rbkmoney.analytics.exception.AdjustmentInfoNotFoundException;
import com.rbkmoney.analytics.service.GeoProvider;
import com.rbkmoney.damsel.domain.FinalCashFlowPosting;
import com.rbkmoney.damsel.domain.Invoice;
import com.rbkmoney.damsel.payment_processing.InvoicePayment;
import com.rbkmoney.damsel.payment_processing.InvoicePaymentRefund;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;

@Slf4j
@Service
public class RefundRowFactory extends InvoiceBaseRowFactory<RefundRow> {

    private final CashFlowComputer cashFlowComputer;

    public RefundRowFactory(GeoProvider geoProvider, CashFlowComputer cashFlowComputer) {
        super(geoProvider);
        this.cashFlowComputer = cashFlowComputer;
    }

    @Override
    public RefundRow create(MachineEvent machineEvent, InvoicePaymentWrapper invoicePaymentWrapper, String refundId) {
        InvoicePayment invoicePayment = invoicePaymentWrapper.getInvoicePayment();
        return invoicePayment.getRefunds().stream()
                .filter(refund -> refund.getRefund().getId().equals(refundId))
                .findFirst()
                .map(refund -> mapRow(machineEvent, invoicePayment, invoicePaymentWrapper.getInvoice(), refundId,
                        refund))
                .orElseThrow(AdjustmentInfoNotFoundException::new);
    }

    private RefundRow mapRow(MachineEvent machineEvent, InvoicePayment payment, Invoice invoice,
                             String refundId, InvoicePaymentRefund refund) {
        RefundRow row = new RefundRow();
        List<FinalCashFlowPosting> cashFlow = refund.isSetCashFlow() ? refund.getCashFlow() : payment.getCashFlow();
        row.setRefundId(refundId);
        row.setPaymentId(payment.getPayment().getId());
        row.setCashFlowResult(cashFlowComputer.compute(cashFlow));
        initBaseRow(machineEvent, row, payment, invoice);
        return row;
    }

}
