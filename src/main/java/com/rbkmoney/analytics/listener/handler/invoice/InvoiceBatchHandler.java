package com.rbkmoney.analytics.listener.handler.invoice;

import com.rbkmoney.analytics.listener.handler.BatchHandler;
import com.rbkmoney.damsel.payment_processing.InvoiceChange;
import com.rbkmoney.machinegun.eventsink.MachineEvent;

public interface InvoiceBatchHandler extends BatchHandler<InvoiceChange, MachineEvent> {
}
