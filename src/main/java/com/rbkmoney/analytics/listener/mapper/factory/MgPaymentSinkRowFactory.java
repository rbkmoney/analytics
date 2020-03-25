package com.rbkmoney.analytics.listener.mapper.factory;

import com.rbkmoney.analytics.computer.CashFlowComputer;
import com.rbkmoney.analytics.dao.model.MgPaymentSinkRow;
import com.rbkmoney.analytics.domain.InvoicePaymentWrapper;
import com.rbkmoney.analytics.service.GeoProvider;
import com.rbkmoney.damsel.domain.FinalCashFlowPosting;
import com.rbkmoney.damsel.domain.Invoice;
import com.rbkmoney.damsel.payment_processing.InvoicePayment;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;

@Slf4j
@Service
public class MgPaymentSinkRowFactory extends MgBaseRowFactory<MgPaymentSinkRow> {

    private final CashFlowComputer cashFlowComputer;

    public MgPaymentSinkRowFactory(GeoProvider geoProvider, CashFlowComputer cashFlowComputer) {
        super(geoProvider);
        this.cashFlowComputer = cashFlowComputer;
    }

    @Override
    public MgPaymentSinkRow create(MachineEvent machineEvent, InvoicePaymentWrapper invoicePaymentWrapper, String paymentId) {
        MgPaymentSinkRow mgPaymentSinkRow = new MgPaymentSinkRow();
        Invoice invoice = invoicePaymentWrapper.getInvoice();
        mgPaymentSinkRow.setPartyId(invoice.getOwnerId());
        mgPaymentSinkRow.setShopId(invoice.getShopId());
        mgPaymentSinkRow.setInvoiceId(machineEvent.getSourceId());
        mgPaymentSinkRow.setPaymentId(paymentId);
        mgPaymentSinkRow.setSequenceId((machineEvent.getEventId()));
        initInfo(machineEvent, mgPaymentSinkRow, invoicePaymentWrapper.getInvoicePayment());
        return mgPaymentSinkRow;
    }

    private void initInfo(MachineEvent machineEvent, MgPaymentSinkRow row, InvoicePayment payment) {
        List<FinalCashFlowPosting> cashFlow = payment.getCashFlow();
        row.setCashFlowResult(cashFlowComputer.compute(cashFlow));
        initBaseRow(machineEvent, row, payment);
    }

}
