package com.rbkmoney.analytics.listener.mapper.factory;

import com.rbkmoney.analytics.computer.CashFlowComputer;
import com.rbkmoney.analytics.computer.ReversedCashFlowComputer;
import com.rbkmoney.analytics.dao.model.MgAdjustmentRow;
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
public class MgAdjustmentRowFactory extends MgBaseRowFactory<MgAdjustmentRow> {

    private final CashFlowComputer cashFlowComputer;
    private final ReversedCashFlowComputer reversedCashFlowComputer;

    public MgAdjustmentRowFactory(GeoProvider geoProvider, CashFlowComputer cashFlowComputer, ReversedCashFlowComputer reversedCashFlowComputer) {
        super(geoProvider);
        this.cashFlowComputer = cashFlowComputer;
        this.reversedCashFlowComputer = reversedCashFlowComputer;
    }

    @Override
    public MgAdjustmentRow create(MachineEvent machineEvent, InvoicePaymentWrapper invoicePaymentWrapper, String adjustmentId) {
        MgAdjustmentRow mgAdjustmentRow = new MgAdjustmentRow();
        Invoice invoice = invoicePaymentWrapper.getInvoice();
        InvoicePayment payment = invoicePaymentWrapper.getInvoicePayment();
        payment.getAdjustments().stream()
                .filter(adjustment -> adjustment.getId().equals(adjustmentId))
                .findFirst()
                .ifPresentOrElse(adjustment -> mapRow(machineEvent, mgAdjustmentRow, payment, invoice, adjustmentId, adjustment), () -> {
                            throw new AdjustmentInfoNotFoundException();
                        }
                );
        return mgAdjustmentRow;
    }

    private void mapRow(MachineEvent machineEvent, MgAdjustmentRow row, InvoicePayment payment, Invoice invoice, String id, InvoicePaymentAdjustment adjustment) {
        row.setAdjustmentId(id);
        row.setPaymentId(payment.getPayment().getId());
        initBaseRow(machineEvent, row, payment, invoice);

        List<FinalCashFlowPosting> cashFlow = adjustment.getNewCashFlow();
        row.setCashFlowResult(cashFlowComputer.compute(cashFlow));

        List<FinalCashFlowPosting> oldCashFlow = adjustment.getNewCashFlow();
        row.setOldCashFlowResult(reversedCashFlowComputer.compute(oldCashFlow));
    }

}
