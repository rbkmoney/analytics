package com.rbkmoney.analytics.listener.mapper;

import com.rbkmoney.damsel.payment_processing.Invoice;
import com.rbkmoney.damsel.payment_processing.InvoicePayment;

import java.util.Optional;
import java.util.function.BiFunction;

public abstract class AbstractMapper<C, P, R> implements Mapper<C, P, R> {

    protected BiFunction<String, Invoice, Optional<InvoicePayment>> findPayment() {
        return (id, invoiceInfo) -> invoiceInfo.getPayments().stream()
                .filter(payment -> payment.isSetPayment() && payment.getPayment().getId().equals(id))
                .findFirst();
    }

}
