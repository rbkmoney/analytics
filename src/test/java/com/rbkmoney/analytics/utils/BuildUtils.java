package com.rbkmoney.analytics.utils;

import com.rbkmoney.damsel.base.Content;
import com.rbkmoney.damsel.domain.*;
import com.rbkmoney.damsel.payment_processing.InvoicePayment;
import com.rbkmoney.damsel.payment_processing.InvoiceRefundSession;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.geck.serializer.kit.mock.MockMode;
import com.rbkmoney.geck.serializer.kit.mock.MockTBaseProcessor;
import com.rbkmoney.geck.serializer.kit.tbase.TBaseHandler;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class BuildUtils {
    private static int messageId = 1;

    public static com.rbkmoney.damsel.payment_processing.Invoice buildInvoice(String partyId, String shopId, String invoiceId, String paymentId, String refundId, String adjustmentId,
                                                                              InvoiceStatus invoiceStatus, InvoicePaymentStatus paymentStatus) throws IOException {
        MockTBaseProcessor tBaseProcessor = new MockTBaseProcessor(MockMode.RANDOM, 15, 1);
        com.rbkmoney.damsel.payment_processing.Invoice invoice = new com.rbkmoney.damsel.payment_processing.Invoice()
                .setInvoice(buildInvoice(partyId, shopId, invoiceId, invoiceStatus, tBaseProcessor))
                .setPayments(buildPayments(partyId, shopId, paymentId, refundId, adjustmentId, paymentStatus, tBaseProcessor));
        if (invoice.getPayments().get(0).getPayment().getPayer().isSetPaymentResource()) {
            invoice.getPayments().get(0).getPayment().getPayer().getPaymentResource().getResource()
                    .setPaymentTool(PaymentTool.bank_card(tBaseProcessor.process(new BankCard(), new TBaseHandler<>(BankCard.class))));
        }
        return invoice;
    }

    private static Invoice buildInvoice(String partyId, String shopId, String invoiceId, InvoiceStatus invoiceStatus, MockTBaseProcessor tBaseProcessor) throws IOException {
        return tBaseProcessor.process(
                new Invoice(),
                new TBaseHandler<>(Invoice.class)
        )
                .setId(invoiceId)
                .setShopId(shopId)
                .setOwnerId(partyId)
                .setCreatedAt(TypeUtil.temporalToString(Instant.now()))
                .setContext(new Content("lel", ByteBuffer.wrap("{\"payment_id\": 271771960}".getBytes())))
                .setDue("2016-03-22T06:12:27Z")
                .setStatus(invoiceStatus);
    }

    private static List<InvoicePayment> buildPayments(String partyId, String shopId, String paymentId, String refundId, String adjustmentId, InvoicePaymentStatus paymentStatus, MockTBaseProcessor tBaseProcessor) throws IOException {
        return Collections.singletonList(
                new InvoicePayment()
                        .setAdjustments(List.of(buildAdjustment(adjustmentId, tBaseProcessor)))
                        .setPayment(buildPayment(partyId, shopId, paymentId, paymentStatus, tBaseProcessor))
                        .setRefunds(buildRefunds(refundId, tBaseProcessor))
                        .setCashFlow(List.of(
                                payment(1000L),
                                systemFee(100L),
                                providerFee(20L),
                                externalFee(10L),
                                guaranteeDeposit(300L),
                                incorrectPosting(99_999L)))
                        .setSessions(Collections.emptyList())
        );
    }


    private static FinalCashFlowPosting payment(long amount) {
        return new FinalCashFlowPosting()
                .setSource(
                        new FinalCashFlowAccount()
                                .setAccountType(
                                        new CashFlowAccount(
                                                CashFlowAccount.provider(
                                                        ProviderCashFlowAccount.settlement))))
                .setDestination(
                        new FinalCashFlowAccount()
                                .setAccountId(1L)
                                .setAccountType(
                                        new CashFlowAccount(
                                                CashFlowAccount.merchant(
                                                        MerchantCashFlowAccount.settlement))))
                .setVolume(new Cash()
                        .setAmount(amount));
    }

    private static FinalCashFlowPosting refund(long amount) {
        return new FinalCashFlowPosting()
                .setSource(
                        new FinalCashFlowAccount()
                                .setAccountId(1L)
                                .setAccountType(
                                        new CashFlowAccount(
                                                CashFlowAccount.merchant(
                                                        MerchantCashFlowAccount.settlement))))
                .setDestination(
                        new FinalCashFlowAccount()
                                .setAccountType(
                                        new CashFlowAccount(
                                                CashFlowAccount.provider(
                                                        ProviderCashFlowAccount.settlement))))
                .setVolume(new Cash()
                        .setAmount(amount));
    }

    private static FinalCashFlowPosting systemFee(long amount) {
        return new FinalCashFlowPosting()
                .setSource(
                        new FinalCashFlowAccount()
                                .setAccountType(
                                        new CashFlowAccount(
                                                CashFlowAccount.merchant(
                                                        MerchantCashFlowAccount.settlement))))
                .setDestination(
                        new FinalCashFlowAccount()
                                .setAccountId(1L)
                                .setAccountType(
                                        new CashFlowAccount(
                                                CashFlowAccount.system(
                                                        SystemCashFlowAccount.settlement))))
                .setVolume(new Cash()
                        .setAmount(amount));
    }

    private static FinalCashFlowPosting providerFee(long amount) {
        return new FinalCashFlowPosting()
                .setSource(
                        new FinalCashFlowAccount()
                                .setAccountType(
                                        new CashFlowAccount(
                                                CashFlowAccount.system(
                                                        SystemCashFlowAccount.settlement))))
                .setDestination(
                        new FinalCashFlowAccount()
                                .setAccountId(1L)
                                .setAccountType(
                                        new CashFlowAccount(
                                                CashFlowAccount.provider(
                                                        ProviderCashFlowAccount.settlement))))
                .setVolume(new Cash()
                        .setAmount(amount));
    }

    private static FinalCashFlowPosting externalFee(long amount) {
        return new FinalCashFlowPosting()
                .setSource(
                        new FinalCashFlowAccount()
                                .setAccountType(
                                        new CashFlowAccount(
                                                CashFlowAccount.system(
                                                        SystemCashFlowAccount.settlement))))
                .setDestination(
                        new FinalCashFlowAccount()
                                .setAccountId(1L)
                                .setAccountType(
                                        new CashFlowAccount(
                                                CashFlowAccount.external(
                                                        ExternalCashFlowAccount.outcome))))
                .setVolume(new Cash()
                        .setAmount(amount));
    }

    private static FinalCashFlowPosting guaranteeDeposit(long amount) {
        return new FinalCashFlowPosting()
                .setSource(
                        new FinalCashFlowAccount()
                                .setAccountType(
                                        new CashFlowAccount(
                                                CashFlowAccount.merchant(
                                                        MerchantCashFlowAccount.settlement))))
                .setDestination(
                        new FinalCashFlowAccount()
                                .setAccountId(1L)
                                .setAccountType(
                                        new CashFlowAccount(
                                                CashFlowAccount.merchant(
                                                        MerchantCashFlowAccount.guarantee))))
                .setVolume(new Cash()
                        .setAmount(amount));
    }


    private static FinalCashFlowPosting incorrectPosting(long amount) {
        return new FinalCashFlowPosting()
                .setDestination(
                        new FinalCashFlowAccount()
                                .setAccountId(1L)
                                .setAccountType(
                                        new CashFlowAccount(
                                                CashFlowAccount.merchant(
                                                        MerchantCashFlowAccount.settlement))))
                .setVolume(new Cash()
                        .setAmount(amount));
    }

    private static com.rbkmoney.damsel.domain.InvoicePayment buildPayment(String partyId, String shopId, String paymentId, InvoicePaymentStatus paymentStatus, MockTBaseProcessor tBaseProcessor) throws IOException {
        return tBaseProcessor.process(
                new com.rbkmoney.damsel.domain.InvoicePayment(),
                new TBaseHandler<>(com.rbkmoney.damsel.domain.InvoicePayment.class)
        )
                .setCreatedAt("2016-03-22T06:12:27Z")
                .setId(paymentId)
                .setOwnerId(partyId)
                .setShopId(shopId)
                .setCost(new Cash()
                        .setAmount(123L)
                        .setCurrency(new CurrencyRef("RUB"))
                )
                .setStatus(paymentStatus);
    }

    private static List<com.rbkmoney.damsel.payment_processing.InvoicePaymentRefund> buildRefunds(String refundId, MockTBaseProcessor tBaseProcessor) throws IOException {
        com.rbkmoney.damsel.payment_processing.InvoicePaymentRefund invoicePaymentRefund = new com.rbkmoney.damsel.payment_processing.InvoicePaymentRefund(
                buildRefund(refundId, tBaseProcessor),
                Collections.singletonList(new InvoiceRefundSession().setTransactionInfo(getTransactionInfo()))
        );
        invoicePaymentRefund.setCashFlow(List.of(
                payment(123L),
                systemFee(100L),
                providerFee(20L),
                externalFee(10L),
                guaranteeDeposit(100L),
                incorrectPosting(99_999L)));
        return Collections.singletonList(
                invoicePaymentRefund
        );
    }

    private static InvoicePaymentRefund buildRefund(String refundId, MockTBaseProcessor tBaseProcessor) throws IOException {
        return tBaseProcessor.process(
                new InvoicePaymentRefund(),
                new TBaseHandler<>(InvoicePaymentRefund.class)
        )
                .setReason("keksik")
                .setCreatedAt(TypeUtil.temporalToString(Instant.now()))
                .setId(refundId);
    }


    private static InvoicePaymentAdjustment buildAdjustment(String adjustmentId, MockTBaseProcessor tBaseProcessor) throws IOException {
        return tBaseProcessor.process(
                new InvoicePaymentAdjustment(),
                new TBaseHandler<>(InvoicePaymentAdjustment.class)
        )
                .setReason("keksik")
                .setCreatedAt(TypeUtil.temporalToString(Instant.now()))
                .setNewCashFlow(List.of(
                        payment(23L),
                        systemFee(100L),
                        providerFee(20L),
                        externalFee(10L),
                        guaranteeDeposit(100L),
                        incorrectPosting(99_999L)))
                .setOldCashFlowInverse(List.of(
                        payment(123L),
                        systemFee(100L),
                        providerFee(20L),
                        externalFee(10L),
                        guaranteeDeposit(100L),
                        incorrectPosting(99_999L)))
                .setId(adjustmentId);
    }

    private static TransactionInfo getTransactionInfo() {
        return new TransactionInfo()
                .setId(UUID.randomUUID().toString())
                .setExtra(Map.of())
                .setAdditionalInfo(getAdditionalInfo());
    }

    private static AdditionalTransactionInfo getAdditionalInfo() {
        return new AdditionalTransactionInfo()
                .setRrn("chicken-teriyaki");
    }

}
