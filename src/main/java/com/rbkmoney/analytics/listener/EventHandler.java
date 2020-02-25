package com.rbkmoney.analytics.listener;

import com.rbkmoney.damsel.payment_processing.InvoiceChange;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import org.apache.thrift.TException;

public interface EventHandler<R> {

    R handle(InvoiceChange change, MachineEvent machineEvent) throws TException;

}
