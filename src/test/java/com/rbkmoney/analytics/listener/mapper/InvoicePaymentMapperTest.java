package com.rbkmoney.analytics.listener.mapper;

import com.rbkmoney.analytics.MgEventSinkFlowGenerator;
import com.rbkmoney.analytics.dao.model.MgPaymentSinkRow;
import com.rbkmoney.analytics.domain.CashFlowResult;
import com.rbkmoney.analytics.domain.InvoicePaymentWrapper;
import com.rbkmoney.analytics.listener.mapper.factory.MgPaymentSinkRowFactory;
import com.rbkmoney.analytics.service.HgClientService;
import com.rbkmoney.analytics.utils.EventRangeFactory;
import com.rbkmoney.damsel.domain.Cash;
import com.rbkmoney.damsel.domain.CurrencyRef;
import com.rbkmoney.damsel.payment_processing.Invoice;
import com.rbkmoney.damsel.payment_processing.InvoicePayment;
import com.rbkmoney.damsel.payment_processing.InvoicingSrv;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import org.apache.thrift.TException;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;


public class InvoicePaymentMapperTest {

    public static final long AMOUNT = 123L;
    HgClientService hgClientService;
    @Mock
    MgPaymentSinkRowFactory mgPaymentSinkRowFactory;

    private InvoicePaymentMapper invoicePaymentMapper;
    @Mock
    private InvoicingSrv.Iface invoicingClient;
    @Mock
    private EventRangeFactory eventRangeFactory;

    @Before
    public void init() {
        MockitoAnnotations.initMocks(this);
        hgClientService = new HgClientService(invoicingClient, eventRangeFactory);
        invoicePaymentMapper = new InvoicePaymentMapper(hgClientService, mgPaymentSinkRowFactory);
    }

    @Test
    public void map() throws TException {
        String paymentId = "test";
        when(invoicingClient.get(any(), any(), any())).thenReturn(createInvoice(paymentId));
        MgPaymentSinkRow paymentSinkRow = new MgPaymentSinkRow();
        paymentSinkRow.setCashFlowResult(CashFlowResult.EMPTY);
        when(mgPaymentSinkRowFactory.create(any(), any(), any())).thenReturn(paymentSinkRow);
        MgPaymentSinkRow sinkRow = invoicePaymentMapper.map(
                MgEventSinkFlowGenerator.createInvoiceFailed(paymentId), new MachineEvent());

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

    @NotNull
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