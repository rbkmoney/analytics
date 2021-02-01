package com.rbkmoney.analytics.listener.handler.party;

import com.rbkmoney.damsel.payment_processing.PartyChange;
import com.rbkmoney.machinegun.eventsink.MachineEvent;


public interface PartyManagementEventHandler {

    void handle(MachineEvent machineEvent, PartyChange change);

}
