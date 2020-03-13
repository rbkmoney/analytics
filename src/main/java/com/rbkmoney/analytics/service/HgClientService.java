package com.rbkmoney.analytics.service;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.rbkmoney.analytics.domain.InvoicePaymentWrapper;
import com.rbkmoney.analytics.exception.PaymentInfoNotFoundException;
import com.rbkmoney.analytics.exception.PaymentInfoRequestException;
import com.rbkmoney.analytics.utils.EventRangeFactory;
import com.rbkmoney.damsel.payment_processing.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.springframework.stereotype.Service;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

@Slf4j
@Service
@RequiredArgsConstructor
public class HgClientService {

    public static final String ANALYTICS = "analytics";
    public static final UserInfo USER_INFO = new UserInfo(ANALYTICS, UserType.service_user(new ServiceUser()));

    private final InvoicingSrv.Iface invoicingClient;
    private final EventRangeFactory eventRangeFactory;

    private Cache<String, InvoicePaymentWrapper> cache = Caffeine.newBuilder()
            .expireAfterWrite(5, TimeUnit.MINUTES)
            .maximumSize(2000)
            .build();

    public InvoicePaymentWrapper getInvoiceInfo(String invoiceId,
                                                BiFunction<String, com.rbkmoney.damsel.payment_processing.Invoice, Optional<InvoicePayment>> findPaymentPredicate,
                                                String id, long sequenceId) {
        InvoicePaymentWrapper invoicePaymentWrapper = cache.getIfPresent(generateKey(invoiceId, sequenceId));
        if (invoicePaymentWrapper == null) {
            invoicePaymentWrapper = new InvoicePaymentWrapper();
            com.rbkmoney.damsel.payment_processing.Invoice invoiceInfo = invokeHg(invoiceId, sequenceId);
            if (invoiceInfo == null) {
                throw new PaymentInfoNotFoundException("Not found invoice info in hg!");
            }
            invoicePaymentWrapper.setInvoice(invoiceInfo.getInvoice());
            findPaymentPredicate.apply(id, invoiceInfo)
                    .ifPresentOrElse(invoicePaymentWrapper::setInvoicePayment, () -> {
                        throw new PaymentInfoNotFoundException("Not found payment in invoice!");
                    });
            cache.put(generateKey(invoiceId, sequenceId), invoicePaymentWrapper);
        }
        return invoicePaymentWrapper;
    }

    private String generateKey(String invoiceId, long sequenceId) {
        return invoiceId + "_" + sequenceId;
    }

    private Invoice invokeHg(String invoiceId, long sequenceId) {
        try {
            return invoicingClient.get(USER_INFO, invoiceId, eventRangeFactory.create(sequenceId));
        } catch (TException e) {
            log.error("Error when HgClientService getInvoiceInfo e: ", e);
            throw new PaymentInfoRequestException(e);
        }
    }
}
