package com.rbkmoney.analytics.listener.mapper.factory;

import com.rbkmoney.analytics.computer.CashFlowComputer;
import com.rbkmoney.analytics.computer.ReversedCashFlowComputer;
import com.rbkmoney.analytics.dao.model.MgAdjustmentRow;
import com.rbkmoney.analytics.service.GeoProvider;
import com.rbkmoney.damsel.domain.FinalCashFlowPosting;
import com.rbkmoney.damsel.domain.Invoice;
import com.rbkmoney.damsel.domain.InvoicePaymentAdjustment;
import com.rbkmoney.damsel.payment_processing.InvoicePayment;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

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
    public MgAdjustmentRow create(MachineEvent machineEvent, com.rbkmoney.damsel.payment_processing.Invoice invoiceInfo,
                                  String adjustmentId) {
        MgAdjustmentRow mgAdjustmentRow = new MgAdjustmentRow();
        Invoice invoice = invoiceInfo.getInvoice();
        mgAdjustmentRow.setPartyId(invoice.getOwnerId());
        mgAdjustmentRow.setShopId(invoice.getShopId());
        mgAdjustmentRow.setInvoiceId(machineEvent.getSourceId());
        mgAdjustmentRow.setSequenceId((machineEvent.getEventId()));
        initInfo(machineEvent, mgAdjustmentRow, invoiceInfo, adjustmentId);
        return mgAdjustmentRow;
    }

    private void initInfo(MachineEvent machineEvent, MgAdjustmentRow row, com.rbkmoney.damsel.payment_processing.Invoice invoiceInfo, String id) {
        for (InvoicePayment payment : invoiceInfo.getPayments()) {
            if (payment.isSetPayment() && payment.isSetAdjustments()) {
                for (InvoicePaymentAdjustment adjustment : payment.getAdjustments()) {
                    if (adjustment.getId().equals(id)) {
                        List<FinalCashFlowPosting> cashFlow = adjustment.getNewCashFlow();
                        row.setAdjustmentId(id);
                        row.setPaymentId(payment.getPayment().getId());
                        cashFlowComputer.compute(cashFlow)
                                .ifPresent(row::setCashFlowResult);
                        initBaseRow(machineEvent, row, payment);
                        List<FinalCashFlowPosting> oldCashFlow = adjustment.getNewCashFlow();
                        if (!CollectionUtils.isEmpty(oldCashFlow)) {
                            reversedCashFlowComputer.compute(oldCashFlow)
                                    .ifPresent(row::setOldCashFlowResult);
                        }
                    }
                }
            }
        }
    }

}
