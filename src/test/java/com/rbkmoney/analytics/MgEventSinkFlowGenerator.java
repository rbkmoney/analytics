package com.rbkmoney.analytics;


import com.rbkmoney.damsel.base.Content;
import com.rbkmoney.damsel.domain.InvoicePaymentPending;
import com.rbkmoney.damsel.domain.InvoicePaymentRefundPending;
import com.rbkmoney.damsel.domain.*;
import com.rbkmoney.damsel.payment_processing.*;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.kafka.common.serialization.ThriftSerializer;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import com.rbkmoney.machinegun.eventsink.SinkEvent;
import com.rbkmoney.machinegun.msgpack.Value;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class MgEventSinkFlowGenerator {

    private static final String SOURCE_NS = "source_ns";
    private static final String PAYMENT_ID = "1";
    public static final String SHOP_ID = "SHOP_ID";
    private static final String TEST_MAIL_RU = "test@mail.ru";
    private static final String BIN = "666";
    public static final String PARTY_ID = "owner_id";
    public static final String REFUND_ID = "1";

    public static List<SinkEvent> generateSuccessFlow(String sourceId) {
        List<SinkEvent> sinkEvents = new ArrayList<>();
        Long sequenceId = 0L;
        sinkEvents.add(createSinkEvent(createMessageCreateInvoice(sourceId, sequenceId++)));
        sinkEvents.add(createSinkEvent(createMessagePaymentPending(sourceId, sequenceId++)));
        sinkEvents.add(createSinkEvent(createMessagePaymentPending(sourceId, sequenceId++)));
        sinkEvents.add(createSinkEvent(createMessagePaymentPending(sourceId, sequenceId++)));
        sinkEvents.add(createSinkEvent(createMessagePaymentProcessed(sourceId, sequenceId++)));
        sinkEvents.add(createSinkEvent(createMessagePaymentProcessed(sourceId, sequenceId++)));
        sinkEvents.add(createSinkEvent(createMessageInvoiceCaptured(sourceId, sequenceId)));
        return sinkEvents;
    }

    public static List<SinkEvent> generateSuccessNotFullFlow(String sourceId) {
        List<SinkEvent> sinkEvents = new ArrayList<>();
        Long sequenceId = 0L;
        sinkEvents.add(createSinkEvent(createMessageCreateInvoice(sourceId, sequenceId++)));
        sinkEvents.add(createSinkEvent(createMessagePaymentPending(sourceId, sequenceId++)));
        sinkEvents.add(createSinkEvent(createMessagePaymentPending(sourceId, sequenceId++)));
        sinkEvents.add(createSinkEvent(createMessagePaymentPending(sourceId, sequenceId++)));
        sinkEvents.add(createSinkEvent(createMessagePaymentProcessed(sourceId, sequenceId++)));
        return sinkEvents;
    }

    public static List<SinkEvent> generateRefundedFlow(String sourceId) {
        List<SinkEvent> sinkEvents = new ArrayList<>();
        Long sequenceId = 0L;
        sinkEvents.add(createSinkEvent(createMessageCreateInvoice(sourceId, sequenceId++)));
        sinkEvents.add(createSinkEvent(createMessagePaymentPending(sourceId, sequenceId++)));
        sinkEvents.add(createSinkEvent(createMessagePaymentPendingChangeStatus(sourceId, sequenceId++)));
        sinkEvents.add(createSinkEvent(createMessagePaymentPendingChangeStatus(sourceId, sequenceId++)));
        sinkEvents.add(createSinkEvent(createMessagePaymentProcessed(sourceId, sequenceId++)));
        sinkEvents.add(createSinkEvent(createMessagePaymentProcessed(sourceId, sequenceId++)));
        sinkEvents.add(createSinkEvent(createMessageInvoiceCaptured(sourceId, sequenceId++)));
        sinkEvents.add(createSinkEvent(createRefundMessageCreateInvoice(sourceId, sequenceId++)));
        sinkEvents.add(createSinkEvent(statusChangeRefundMessageCreateInvoice(sourceId, sequenceId)));

        return sinkEvents;
    }

    private static SinkEvent createSinkEvent(MachineEvent machineEvent) {
        SinkEvent sinkEvent = new SinkEvent();
        sinkEvent.setEvent(machineEvent);
        return sinkEvent;
    }

    private static MachineEvent createMessageCreateInvoice(String sourceId, Long sequenceId) {
        InvoiceCreated invoiceCreated = createInvoiceCreate(sourceId);
        InvoiceChange invoiceChange = new InvoiceChange();
        invoiceChange.setInvoiceCreated(invoiceCreated);
        return createMachineEvent(invoiceChange, sourceId, sequenceId);
    }

    private static MachineEvent createRefundMessageCreateInvoice(String sourceId, Long sequenceId) {
        InvoicePaymentRefundChange invoicePaymentRefundCreated = new InvoicePaymentRefundChange()
                .setId(REFUND_ID)
                .setPayload(InvoicePaymentRefundChangePayload.invoice_payment_refund_created(
                        new InvoicePaymentRefundCreated()
                                .setRefund(new InvoicePaymentRefund()
                                        .setCreatedAt(TypeUtil.temporalToString(Instant.now()))
                                        .setId(REFUND_ID)
                                        .setReason("refund reason")
                                        .setCash(createCash())
                                        .setStatus(InvoicePaymentRefundStatus.pending(new InvoicePaymentRefundPending()))
                                )
                                .setCashFlow(new ArrayList<>())
                        )
                );

        InvoiceChange invoiceChange = InvoiceChange.invoice_payment_change(new InvoicePaymentChange()
                .setId(PAYMENT_ID)
                .setPayload(InvoicePaymentChangePayload.invoice_payment_refund_change(invoicePaymentRefundCreated))
        );
        return createMachineEvent(invoiceChange, sourceId, sequenceId);
    }


    private static MachineEvent statusChangeRefundMessageCreateInvoice(String sourceId, Long sequenceId) {
        InvoicePaymentRefundChange invoicePaymentRefundCreated = new InvoicePaymentRefundChange()
                .setId(REFUND_ID)
                .setPayload(InvoicePaymentRefundChangePayload.invoice_payment_refund_status_changed(
                       new InvoicePaymentRefundStatusChanged( InvoicePaymentRefundStatus.succeeded(new InvoicePaymentRefundSucceeded())
                        ))
                );

        InvoiceChange invoiceChange = InvoiceChange.invoice_payment_change(new InvoicePaymentChange()
                .setId(PAYMENT_ID)
                .setPayload(InvoicePaymentChangePayload.invoice_payment_refund_change(invoicePaymentRefundCreated))
        );
        return createMachineEvent(invoiceChange, sourceId, sequenceId);
    }

    private static MachineEvent createMessageInvoiceCaptured(String sourceId, Long sequenceId) {
        InvoiceChange invoiceCaptured = createInvoiceCaptured();
        return createMachineEvent(invoiceCaptured, sourceId, sequenceId);
    }

    private static MachineEvent createMessagePaymentProcessed(String sourceId, Long sequenceId) {
        InvoiceChange invoiceCaptured = createPaymentProcessedChange();
        return createMachineEvent(invoiceCaptured, sourceId, sequenceId);
    }

    private static MachineEvent createMessagePaymentPending(String sourceId, Long sequenceId) {
        InvoiceChange paymentPending = createPaymentPending();
        return createMachineEvent(paymentPending, sourceId, sequenceId);
    }

    private static MachineEvent createMessagePaymentPendingChangeStatus(String sourceId, Long sequenceId) {
        InvoiceChange paymentPending = createPaymentPendingStatus();
        return createMachineEvent(paymentPending, sourceId, sequenceId);
    }

    private static MachineEvent createMachineEvent(InvoiceChange invoiceChange, String sourceId, Long sequenceId) {
        MachineEvent message = new MachineEvent();
        EventPayload payload = new EventPayload();
        ArrayList<InvoiceChange> invoiceChanges = new ArrayList<>();
        invoiceChanges.add(invoiceChange);
        payload.setInvoiceChanges(invoiceChanges);

        message.setCreatedAt(TypeUtil.temporalToString(Instant.now()));
        message.setEventId(sequenceId);
        message.setSourceNs(SOURCE_NS);
        message.setSourceId(sourceId);

        ThriftSerializer<EventPayload> eventPayloadThriftSerializer = new ThriftSerializer<>();
        Value data = new Value();
        data.setBin(eventPayloadThriftSerializer.serialize("", payload));
        message.setData(data);
        return message;
    }

    private static InvoiceCreated createInvoiceCreate(String sourceId) {

        return new InvoiceCreated()
                .setInvoice(new com.rbkmoney.damsel.domain.Invoice()
                        .setId(sourceId)
                        .setOwnerId(PARTY_ID)
                        .setShopId(SHOP_ID)
                        .setCreatedAt(TypeUtil.temporalToString(Instant.now()))
                        .setStatus(InvoiceStatus.unpaid(new InvoiceUnpaid()))
                        .setDue("2016-08-10T16:07:23Z")
                        .setCost(createCash())
                        .setDetails(new InvoiceDetails("product"))
                        .setContext(new Content()
                                .setType("contentType")
                                .setData("test".getBytes())
                        )
                );
    }

    @NotNull
    private static Cash createCash() {
        return new Cash(12L, new CurrencyRef("RUB"));
    }

    private static InvoiceChange createInvoiceCaptured() {
        InvoiceChange invoiceChange = new InvoiceChange();
        InvoicePaymentChangePayload payload = new InvoicePaymentChangePayload();
        InvoicePaymentStatusChanged invoicePaymentStatusChanged = new InvoicePaymentStatusChanged()
                .setStatus(InvoicePaymentStatus.captured(new InvoicePaymentCaptured()));
        payload.setInvoicePaymentStatusChanged(invoicePaymentStatusChanged);
        invoiceChange.setInvoicePaymentChange(new InvoicePaymentChange()
                .setId("1")
                .setPayload(payload));
        return invoiceChange;
    }

    private static InvoiceChange createPaymentProcessedChange() {
        return createInvoiceChangeChangeStatus(InvoicePaymentStatus.processed(new InvoicePaymentProcessed()));
    }

    private static InvoiceChange createPaymentPending() {
        return createInvoiceChange(InvoicePaymentStatus.pending(new InvoicePaymentPending()));
    }

    private static InvoiceChange createPaymentPendingStatus() {
        return createInvoiceChangeChangeStatus(InvoicePaymentStatus.pending(new InvoicePaymentPending()));
    }

    @NotNull
    private static InvoiceChange createInvoiceChange(InvoicePaymentStatus invoicePaymentStatus) {
        InvoicePaymentChangePayload invoicePaymentChangePayload = new InvoicePaymentChangePayload();
        invoicePaymentChangePayload.setInvoicePaymentStatusChanged(
                new InvoicePaymentStatusChanged(invoicePaymentStatus)
        );
        invoicePaymentChangePayload.setInvoicePaymentStarted(
                new InvoicePaymentStarted()
                        .setPayment(new com.rbkmoney.damsel.domain.InvoicePayment()
                                .setCost(
                                        new Cash()
                                                .setAmount(123L)
                                                .setCurrency(new CurrencyRef()
                                                        .setSymbolicCode("RUB")))
                                .setCreatedAt(TypeUtil.temporalToString(Instant.now()))
                                .setId(PAYMENT_ID)
                                .setStatus(invoicePaymentStatus)
                                .setPayer(createCustomerPayer())
                                .setOwnerId(PARTY_ID)
                                .setShopId(SHOP_ID)
                                .setFlow(createFlow())));
        InvoiceChange invoiceChange = new InvoiceChange();
        invoiceChange.setInvoicePaymentChange(new InvoicePaymentChange()
                .setId(PAYMENT_ID)
                .setPayload(invoicePaymentChangePayload));
        return invoiceChange;
    }

    @NotNull
    private static InvoiceChange createInvoiceChangeChangeStatus(InvoicePaymentStatus invoicePaymentStatus) {
        InvoicePaymentChangePayload invoicePaymentChangePayload = new InvoicePaymentChangePayload();
        invoicePaymentChangePayload.setInvoicePaymentStatusChanged(
                new InvoicePaymentStatusChanged(invoicePaymentStatus)
        );
        InvoiceChange invoiceChange = new InvoiceChange();
        invoiceChange.setInvoicePaymentChange(new InvoicePaymentChange()
                .setId(PAYMENT_ID)
                .setPayload(invoicePaymentChangePayload));
        return invoiceChange;
    }

    @NotNull
    private static InvoicePaymentFlow createFlow() {
        InvoicePaymentFlow flow = new InvoicePaymentFlow();
        InvoicePaymentFlowHold invoicePaymentFlowHold = new InvoicePaymentFlowHold();
        invoicePaymentFlowHold.setOnHoldExpiration(OnHoldExpiration.capture);
        invoicePaymentFlowHold.setHeldUntil("werwer");
        flow.setHold(invoicePaymentFlowHold);
        return flow;
    }

    private static ClientInfo createClientInfo() {
        ClientInfo clientInfo = new ClientInfo();
        clientInfo.setFingerprint("finger");
        clientInfo.setIpAddress("123.123.123.123");
        return clientInfo;
    }

    private static Payer createCustomerPayer() {
        Payer customer = Payer.customer(new CustomerPayer("custId", "1", "rec_paym_tool", createBankCard(), new ContactInfo()));
        customer.setPaymentResource(
                new PaymentResourcePayer()
                        .setResource(new DisposablePaymentResource()
                                .setClientInfo(createClientInfo())
                                .setPaymentTool(createBankCard()))
                        .setContactInfo(new ContactInfo()
                                .setEmail(TEST_MAIL_RU)));
        return customer;
    }

    private static PaymentTool createBankCard() {
        PaymentTool paymentTool = new PaymentTool();
        paymentTool.setBankCard(
                new BankCard()
                        .setToken("477bba133c182267fe5f086924abdc5db71f77bfc27f01f2843f2cdc69d89f05")
                        .setPaymentSystem(BankCardPaymentSystem.mastercard)
                        .setBin(BIN)
                        .setMaskedPan("4242")
                        .setIssuerCountry(Residence.RUS)
        );
        return paymentTool;
    }

}
