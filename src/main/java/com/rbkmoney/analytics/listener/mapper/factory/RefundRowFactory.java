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
        RefundRow refundRow = new RefundRow();
        initInfo(machineEvent, refundRow, invoicePaymentWrapper.getInvoicePayment(), invoicePaymentWrapper.getInvoice(), refundId);
        return refundRow;
    }

    private void initInfo(MachineEvent machineEvent, RefundRow row, InvoicePayment payment, Invoice invoice, String refundId) {
        payment.getRefunds().stream()
                .filter(refund -> refund.getRefund().getId().equals(refundId))
                .findFirst()
                .ifPresentOrElse(refund -> mapRow(machineEvent, row, payment, invoice, refundId, refund), () -> {
                            throw new AdjustmentInfoNotFoundException();
                        }
                );
    }

    private void mapRow(MachineEvent machineEvent, RefundRow row, InvoicePayment payment, Invoice invoice,
                        String refundId, InvoicePaymentRefund refund) {
        List<FinalCashFlowPosting> cashFlow = refund.isSetCashFlow() ? refund.getCashFlow() : payment.getCashFlow();
        row.setRefundId(refundId);
        row.setPaymentId(payment.getPayment().getId());
        row.setCashFlowResult(cashFlowComputer.compute(cashFlow));
        initBaseRow(machineEvent, row, payment, invoice);
    }

}
