package com.rbkmoney.analytics.domain;

import com.rbkmoney.damsel.domain.Invoice;
import com.rbkmoney.damsel.payment_processing.InvoicePayment;
import lombok.Data;

@Data
public class InvoicePaymentWrapper {

    private Invoice invoice;
    private InvoicePayment invoicePayment;

}
