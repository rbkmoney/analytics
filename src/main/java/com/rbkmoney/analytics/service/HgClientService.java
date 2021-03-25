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
import org.springframework.beans.factory.annotation.Value;
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
    public static final String DELIMITER = "_";

    private final InvoicingSrv.Iface invoicingClient;
    private final EventRangeFactory eventRangeFactory;

    @Value("${hg.invoice.cache.expire.time.min:5}")
    private long expireTime;
    @Value("${hg.invoice.cache.max.size:2000}")
    private long maxSize;

    private final Cache<String, InvoicePaymentWrapper> cache = Caffeine.newBuilder()
            .expireAfterWrite(expireTime, TimeUnit.MINUTES)
            .maximumSize(maxSize)
            .build();

    public InvoicePaymentWrapper getInvoiceInfo(String invoiceId,
                                                BiFunction<String, com.rbkmoney.damsel.payment_processing.Invoice,
                                                        Optional<InvoicePayment>> findPaymentPredicate,
                                                String paymentId, String eventId, long sequenceId) {
        return cache.get(generateKey(invoiceId, paymentId, sequenceId),
                generateKey -> getInvoiceFromHg(invoiceId, findPaymentPredicate, eventId, sequenceId));
    }

    public InvoicePaymentWrapper getInvoiceInfo(String invoiceId,
                                                BiFunction<String, com.rbkmoney.damsel.payment_processing.Invoice,
                                                        Optional<InvoicePayment>> findPaymentPredicate,
                                                String paymentId, long sequenceId) {
        return cache.get(generateKey(invoiceId, paymentId, sequenceId),
                generateKey -> getInvoiceFromHg(invoiceId, findPaymentPredicate, paymentId, sequenceId));
    }

    private InvoicePaymentWrapper getInvoiceFromHg(String invoiceId, BiFunction<String, Invoice,
            Optional<InvoicePayment>> findPaymentPredicate,
                                                   String eventId, long sequenceId) {
        InvoicePaymentWrapper invoicePaymentWrapper = new InvoicePaymentWrapper();
        try {
            Invoice invoiceInfo = invoicingClient.get(USER_INFO, invoiceId, eventRangeFactory.create(sequenceId));
            if (invoiceInfo == null) {
                throw new PaymentInfoNotFoundException("Not found invoice info in hg!");
            }
            invoicePaymentWrapper.setInvoice(invoiceInfo.getInvoice());
            findPaymentPredicate.apply(eventId, invoiceInfo)
                    .ifPresentOrElse(invoicePaymentWrapper::setInvoicePayment, () -> {
                        throw new PaymentInfoNotFoundException("Not found payment in invoice!");
                    });
            return invoicePaymentWrapper;
        } catch (TException e) {
            log.error("Error when HgClientService getInvoiceInfo e: ", e);
            throw new PaymentInfoRequestException(e);
        }
    }

    private String generateKey(String invoiceId, String paymentId, long sequenceId) {
        return invoiceId + DELIMITER + paymentId + DELIMITER + sequenceId;
    }

}
