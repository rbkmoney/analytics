package com.rbkmoney.analytics.listener.mapper.factory;

import com.rbkmoney.analytics.dao.model.InvoiceBaseRow;
import com.rbkmoney.analytics.domain.InvoicePaymentWrapper;
import com.rbkmoney.damsel.domain.Invoice;
import com.rbkmoney.damsel.payment_processing.InvoicePayment;
import com.rbkmoney.machinegun.eventsink.MachineEvent;

public interface RowFactory<T extends InvoiceBaseRow> {

    T create(MachineEvent machineEvent, InvoicePaymentWrapper invoiceInfo, String id);

    void initBaseRow(MachineEvent machineEvent, T row, InvoicePayment invoicePayment, Invoice invoice);

}
