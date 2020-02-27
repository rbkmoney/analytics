package com.rbkmoney.analytics.listener.mapper.factory;

import com.rbkmoney.analytics.dao.model.MgAdjustmentRow;
import com.rbkmoney.damsel.domain.FinalCashFlowPosting;
import com.rbkmoney.damsel.domain.Invoice;
import com.rbkmoney.damsel.domain.InvoicePaymentAdjustment;
import com.rbkmoney.damsel.payment_processing.InvoicePayment;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class MgAdjustmentRowFactory extends MgBaseRowFactory<MgAdjustmentRow> {

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
            if (payment.isSetPayment() && payment.isSetRefunds()) {
                for (InvoicePaymentAdjustment adjustment : payment.getAdjustments()) {
                    if (adjustment.getId().equals(id)) {
                        List<FinalCashFlowPosting> cashFlow = adjustment.getNewCashFlow();
                        row.setAdjustmentId(id);
                        row.setPaymentId(payment.getPayment().getId());
                        initCashFlowInfo(row, cashFlow);
                        initBaseRow(machineEvent, row, payment);
                    }
                }
            }
        }
    }

}
