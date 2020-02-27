package com.rbkmoney.analytics.listener.mapper.factory;

import com.rbkmoney.analytics.dao.model.MgPaymentSinkRow;
import com.rbkmoney.damsel.domain.FinalCashFlowPosting;
import com.rbkmoney.damsel.domain.Invoice;
import com.rbkmoney.damsel.payment_processing.InvoicePayment;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class MgPaymentSinkRowFactory extends MgBaseRowFactory<MgPaymentSinkRow> {

    @Override
    public MgPaymentSinkRow create(MachineEvent machineEvent, com.rbkmoney.damsel.payment_processing.Invoice invoiceInfo, String paymentId) {
        MgPaymentSinkRow mgPaymentSinkRow = new MgPaymentSinkRow();
        Invoice invoice = invoiceInfo.getInvoice();
        mgPaymentSinkRow.setPartyId(invoice.getOwnerId());
        mgPaymentSinkRow.setShopId(invoice.getShopId());
        mgPaymentSinkRow.setInvoiceId(machineEvent.getSourceId());
        mgPaymentSinkRow.setPaymentId(paymentId);
        mgPaymentSinkRow.setSequenceId((machineEvent.getEventId()));
        initInfo(machineEvent, mgPaymentSinkRow, invoiceInfo, paymentId);
        return mgPaymentSinkRow;
    }

    private void initInfo(MachineEvent machineEvent, MgPaymentSinkRow row,
                         com.rbkmoney.damsel.payment_processing.Invoice invoiceInfo, String paymentId) {
        for (InvoicePayment payment : invoiceInfo.getPayments()) {
            if (payment.isSetPayment() && payment.getPayment().getId().equals(paymentId)) {
                List<FinalCashFlowPosting> cashFlow = payment.getCashFlow();
                initCashFlowInfo(row, cashFlow);
                initBaseRow(machineEvent, row, payment);
            }
        }
    }

}
