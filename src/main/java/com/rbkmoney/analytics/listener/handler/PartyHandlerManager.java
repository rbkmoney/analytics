package com.rbkmoney.analytics.listener.handler;

import com.rbkmoney.damsel.payment_processing.PartyChange;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@RequiredArgsConstructor
public class PartyHandlerManager {

    private final List<BatchHandler<PartyChange, MachineEvent>> handlers;

    public BatchHandler<PartyChange, MachineEvent> getHandler(PartyChange change) {
        for (BatchHandler<PartyChange, MachineEvent> handler : handlers) {
            if (handler.accept(change)) {
                return handler;
            }
        }
        return null;
    }

}
