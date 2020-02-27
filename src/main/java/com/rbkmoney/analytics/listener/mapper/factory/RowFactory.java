package com.rbkmoney.analytics.listener.mapper.factory;

import com.rbkmoney.analytics.dao.model.MgBaseRow;
import com.rbkmoney.damsel.payment_processing.InvoicePayment;
import com.rbkmoney.machinegun.eventsink.MachineEvent;

public interface RowFactory<T extends MgBaseRow> {

    T create(MachineEvent machineEvent, com.rbkmoney.damsel.payment_processing.Invoice invoiceInfo, String adjustmentId);

    void initBaseRow(MachineEvent machineEvent, T row, InvoicePayment invoicePayment);

}
