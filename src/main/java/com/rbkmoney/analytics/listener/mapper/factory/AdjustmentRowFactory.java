package com.rbkmoney.analytics.listener.mapper.factory;

import com.rbkmoney.analytics.computer.CashFlowComputer;
import com.rbkmoney.analytics.computer.ReversedCashFlowComputer;
import com.rbkmoney.analytics.dao.model.AdjustmentRow;
import com.rbkmoney.analytics.domain.InvoicePaymentWrapper;
import com.rbkmoney.analytics.exception.AdjustmentInfoNotFoundException;
import com.rbkmoney.analytics.service.GeoProvider;
import com.rbkmoney.damsel.domain.FinalCashFlowPosting;
import com.rbkmoney.damsel.domain.Invoice;
import com.rbkmoney.damsel.domain.InvoicePaymentAdjustment;
import com.rbkmoney.damsel.payment_processing.InvoicePayment;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;

@Slf4j
@Service
public class AdjustmentRowFactory extends InvoiceBaseRowFactory<AdjustmentRow> {

    private final CashFlowComputer cashFlowComputer;
    private final ReversedCashFlowComputer reversedCashFlowComputer;

    public AdjustmentRowFactory(GeoProvider geoProvider,
                                CashFlowComputer cashFlowComputer,
                                ReversedCashFlowComputer reversedCashFlowComputer) {
        super(geoProvider);
        this.cashFlowComputer = cashFlowComputer;
        this.reversedCashFlowComputer = reversedCashFlowComputer;
    }

    @Override
    public AdjustmentRow create(MachineEvent machineEvent,
                                InvoicePaymentWrapper invoicePaymentWrapper,
                                String adjustmentId) {
        Invoice invoice = invoicePaymentWrapper.getInvoice();
        InvoicePayment payment = invoicePaymentWrapper.getInvoicePayment();
        return payment.getAdjustments().stream()
                .filter(adjustment -> adjustment.getId().equals(adjustmentId))
                .findFirst()
                .map(adjustment -> mapRow(machineEvent, payment, invoice, adjustmentId, adjustment))
                .orElseThrow(AdjustmentInfoNotFoundException::new);
    }

    private AdjustmentRow mapRow(MachineEvent machineEvent,
                                 InvoicePayment payment,
                                 Invoice invoice,
                                 String id,
                                 InvoicePaymentAdjustment adjustment) {
        AdjustmentRow row = new AdjustmentRow();
        row.setAdjustmentId(id);
        row.setPaymentId(payment.getPayment().getId());
        initBaseRow(machineEvent, row, payment, invoice);

        List<FinalCashFlowPosting> cashFlow = adjustment.getNewCashFlow();
        row.setCashFlowResult(cashFlowComputer.compute(cashFlow));

        List<FinalCashFlowPosting> oldCashFlow = adjustment.getOldCashFlowInverse();
        row.setOldCashFlowResult(reversedCashFlowComputer.compute(oldCashFlow));
        return row;
    }

}
