package com.rbkmoney.analytics.service;

import com.rbkmoney.analytics.exception.PaymentInfoRequestException;
import com.rbkmoney.analytics.utils.EventRangeFactory;
import com.rbkmoney.damsel.payment_processing.*;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class HgClientService {

    public static final String ANALYTICS = "analytics";
    public static final UserInfo USER_INFO = new UserInfo(ANALYTICS, UserType.service_user(new ServiceUser()));

    private final InvoicingSrv.Iface invoicingClient;
    private final EventRangeFactory eventRangeFactory;

    public Invoice getInvoiceInfo(MachineEvent event) {
        try {
            return invoicingClient.get(USER_INFO, event.getSourceId(), eventRangeFactory.create(event.getEventId()));
        } catch (TException e) {
            log.error("Error when HgClientService getInvoiceInfo e: ", e);
            throw new PaymentInfoRequestException(e);
        }
    }

}
