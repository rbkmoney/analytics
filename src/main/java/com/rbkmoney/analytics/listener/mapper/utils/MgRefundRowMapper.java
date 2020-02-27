package com.rbkmoney.analytics.listener.mapper.utils;

import com.rbkmoney.analytics.dao.model.MgRefundRow;
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
public class MgRefundRowMapper extends MgBaseRowMapper<MgRefundRow> {

    public MgRefundRow initInvoiceInfo(MachineEvent machineEvent, com.rbkmoney.damsel.payment_processing.Invoice invoiceInfo, String refundId) {
        MgRefundRow mgPaymentSinkRow = new MgRefundRow();
        Invoice invoice = invoiceInfo.getInvoice();
        mgPaymentSinkRow.setPartyId(invoice.getOwnerId());
        mgPaymentSinkRow.setShopId(invoice.getShopId());
        mgPaymentSinkRow.setInvoiceId(machineEvent.getSourceId());
        mgPaymentSinkRow.setSequenceId((machineEvent.getEventId()));
        initInfo(machineEvent, mgPaymentSinkRow, invoiceInfo, refundId);
        return mgPaymentSinkRow;
    }

    @Override
    public void initInfo(MachineEvent machineEvent, MgRefundRow row,
                         com.rbkmoney.damsel.payment_processing.Invoice invoiceInfo, String refundId) {
        for (InvoicePayment payment : invoiceInfo.getPayments()) {
            if (payment.isSetPayment() && payment.isSetRefunds()) {
                for (com.rbkmoney.damsel.payment_processing.InvoicePaymentRefund refund : payment.getRefunds()) {
                    if (refund.getRefund().getId().equals(refundId)) {
                        List<FinalCashFlowPosting> cashFlow = refund.isSetCashFlow() ? refund.getCashFlow() : payment.getCashFlow();
                        row.setRefundId(refundId);
                        row.setPaymentId(payment.getPayment().getId());
                        initCashFlowInfo(row, cashFlow);
                        initBaseRow(machineEvent, row, payment);
                    }
                }
            }
        }
    }

}
