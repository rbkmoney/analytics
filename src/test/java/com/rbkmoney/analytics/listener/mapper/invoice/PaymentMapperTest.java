package com.rbkmoney.analytics.listener.mapper.invoice;

import com.rbkmoney.analytics.listener.InvoiceFlowGenerator;
import com.rbkmoney.analytics.dao.model.PaymentRow;
import com.rbkmoney.analytics.domain.CashFlowResult;
import com.rbkmoney.analytics.domain.InvoicePaymentWrapper;
import com.rbkmoney.analytics.listener.mapper.factory.PaymentRowFactory;
import com.rbkmoney.analytics.service.HgClientService;
import com.rbkmoney.analytics.utils.EventRangeFactory;
import com.rbkmoney.damsel.domain.Cash;
import com.rbkmoney.damsel.domain.CurrencyRef;
import com.rbkmoney.damsel.payment_processing.Invoice;
import com.rbkmoney.damsel.payment_processing.InvoicePayment;
import com.rbkmoney.damsel.payment_processing.InvoicingSrv;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;


public class PaymentMapperTest {

    public static final long AMOUNT = 123L;

    @Mock
    private PaymentRowFactory paymentRowFactory;
    @Mock
    private InvoicingSrv.Iface invoicingClient;
    @Mock
    private EventRangeFactory eventRangeFactory;

    private PaymentMapper paymentMapper;

    @Before
    public void init() {
        MockitoAnnotations.initMocks(this);
        HgClientService hgClientService = new HgClientService(invoicingClient, eventRangeFactory);
        paymentMapper = new PaymentMapper(hgClientService, paymentRowFactory);
    }

    @Test
    public void map() throws TException {
        String paymentId = "test";
        when(invoicingClient.get(any(), any(), any())).thenReturn(createInvoice(paymentId));
        PaymentRow paymentRow = new PaymentRow();
        paymentRow.setCashFlowResult(CashFlowResult.EMPTY);
        when(paymentRowFactory.create(any(), any(), any())).thenReturn(paymentRow);
        PaymentRow sinkRow = paymentMapper.map(
                InvoiceFlowGenerator.createInvoiceFailed(paymentId), new MachineEvent());

        Assert.assertEquals(AMOUNT, sinkRow.getCashFlowResult().getAmount());
    }

    private Invoice createInvoice(String paymentId) {
        InvoicePayment invoicePayment = createPaymentWrapper(paymentId);
        ArrayList<InvoicePayment> payments = new ArrayList<>();
        payments.add(invoicePayment);
        return new Invoice()
                .setInvoice(new com.rbkmoney.damsel.domain.Invoice())
                .setPayments(payments);
    }

    private InvoicePayment createPaymentWrapper(String paymentId) {
        InvoicePaymentWrapper paymentWrapper = new InvoicePaymentWrapper();
        InvoicePayment invoicePayment = new InvoicePayment();
        invoicePayment.setPayment(new com.rbkmoney.damsel.domain.InvoicePayment()
                .setId(paymentId)
                .setCost(new Cash()
                        .setAmount(AMOUNT)
                        .setCurrency(new CurrencyRef()
                                .setSymbolicCode("RUB"))));
        paymentWrapper.setInvoicePayment(invoicePayment);
        return invoicePayment;
    }
}
