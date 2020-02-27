package com.rbkmoney.analytics.listener.mapper.utils;

import com.rbkmoney.analytics.dao.model.MgBaseRow;
import com.rbkmoney.damsel.payment_processing.Invoice;
import com.rbkmoney.damsel.payment_processing.InvoicePayment;
import com.rbkmoney.machinegun.eventsink.MachineEvent;

public interface RowMapper<T extends MgBaseRow> {

    void initInfo(MachineEvent machineEvent, T row, Invoice invoiceInfo, String refundId);

    void initBaseRow(MachineEvent machineEvent, T row, InvoicePayment invoicePayment);

}
