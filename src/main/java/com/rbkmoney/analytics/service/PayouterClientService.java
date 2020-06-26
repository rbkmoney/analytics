package com.rbkmoney.analytics.service;

import com.rbkmoney.analytics.exception.PayoutInfoNotFoundException;
import com.rbkmoney.damsel.payout_processing.Event;
import com.rbkmoney.damsel.payout_processing.EventRange;
import com.rbkmoney.damsel.payout_processing.PayoutManagementSrv;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.springframework.stereotype.Service;

import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class PayouterClientService {

    private final PayoutManagementSrv.Iface payouterClient;

    public List<Event> getEvents(String payoutId, Long eventId) {
        try {
            log.debug("Looking for aggregate with payoutId={}", payoutId);
            return payouterClient.getEvents(
                    payoutId,
                    new EventRange()
                            .setLimit(eventId.intValue()));
        } catch (TException e) {
            throw new PayoutInfoNotFoundException(e);
        }
    }
}
