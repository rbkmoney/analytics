package com.rbkmoney.analytics.utils;

import com.rbkmoney.analytics.computer.CashFlowComputer;
import com.rbkmoney.analytics.dao.model.MgRefundRow;
import com.rbkmoney.analytics.domain.CashFlowResult;
import com.rbkmoney.damsel.domain.*;
import com.rbkmoney.damsel.payment_processing.InvoicePayment;
import com.rbkmoney.geck.common.util.TypeUtil;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;

import static java.time.ZoneOffset.UTC;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class MgRefundRowUtils {

    public static MgRefundRow initInvoiceInfo(com.rbkmoney.damsel.payment_processing.Invoice invoiceInfo, String refundId) {
        MgRefundRow mgPaymentSinkRow = new MgRefundRow();
        Invoice invoice = invoiceInfo.getInvoice();
        mgPaymentSinkRow.setPartyId(invoice.getOwnerId());
        mgPaymentSinkRow.setShopId(invoice.getShopId());
        initPaymentInfo(mgPaymentSinkRow, invoiceInfo, refundId);
        return mgPaymentSinkRow;
    }

    private static void initPaymentInfo(MgRefundRow mgPaymentSinkRow,
                                        com.rbkmoney.damsel.payment_processing.Invoice invoiceInfo, String refundId) {
        for (InvoicePayment payment : invoiceInfo.getPayments()) {
            if (payment.isSetPayment() && payment.isSetRefunds()) {
                for (com.rbkmoney.damsel.payment_processing.InvoicePaymentRefund refund : payment.getRefunds()) {
                    if (refund.getRefund().getId().equals(refundId)) {
                        List<FinalCashFlowPosting> cashFlow = refund.isSetCashFlow() ? refund.getCashFlow() : payment.getCashFlow();
                        mgPaymentSinkRow.setRefundId(refundId);
                        initCashFlowInfo(mgPaymentSinkRow, cashFlow);
                        initResultRefund(mgPaymentSinkRow, payment, refund);
                    }
                }
            }
        }
    }

    private static void initResultRefund(MgRefundRow mgPaymentSinkRow, InvoicePayment invoicePaymentRefund, com.rbkmoney.damsel.payment_processing.InvoicePaymentRefund refund) {
        LocalDateTime localDateTime = TypeUtil.stringToLocalDateTime(refund.getRefund().getCreatedAt());
        long timestamp = localDateTime.atZone(ZoneOffset.UTC).toInstant().toEpochMilli();

        mgPaymentSinkRow.setTimestamp(java.sql.Date.valueOf(
                Instant.ofEpochMilli(timestamp)
                        .atZone(UTC)
                        .toLocalDate())
        );
        mgPaymentSinkRow.setEventTime(timestamp);
        mgPaymentSinkRow.setEventTimeHour(TimeUtils.parseEventTimeHour(timestamp));
        mgPaymentSinkRow.setCurrency(invoicePaymentRefund.getPayment().getCost().getCurrency().getSymbolicCode());
        mgPaymentSinkRow.setPaymentId(invoicePaymentRefund.getPayment().getId());

        Payer payer = invoicePaymentRefund.getPayment().getPayer();
        if (payer.isSetPaymentResource()) {
            DisposablePaymentResource resource = payer.getPaymentResource().getResource();
            PaymentTool paymentTool = resource.getPaymentTool();
            if (payer.getPaymentResource().isSetResource()) {
                if (resource.isSetClientInfo()) {
                    ClientInfo clientInfo = resource.getClientInfo();
                    mgPaymentSinkRow.setIp(clientInfo.getIpAddress());
                    mgPaymentSinkRow.setFingerprint(clientInfo.getFingerprint());
                }
                initCardData(mgPaymentSinkRow, paymentTool);
            }
            initContactInfo(mgPaymentSinkRow, payer.getPaymentResource().getContactInfo());
        } else if (payer.isSetCustomer()) {
            CustomerPayer customer = payer.getCustomer();
            PaymentTool paymentTool = customer.getPaymentTool();
            initContactInfo(mgPaymentSinkRow, customer.getContactInfo());
            initCardData(mgPaymentSinkRow, paymentTool);
        } else if (payer.isSetRecurrent()) {
            RecurrentPayer recurrent = payer.getRecurrent();
            PaymentTool paymentTool = recurrent.getPaymentTool();
            initCardData(mgPaymentSinkRow, paymentTool);
            initContactInfo(mgPaymentSinkRow, recurrent.getContactInfo());
        } else {
            log.warn("Unkonwn payment tool in payer: {}", payer);
        }
    }

    private static void initCashFlowInfo(MgRefundRow mgRefundRow, List<FinalCashFlowPosting> cashFlow) {
        CashFlowResult compute = CashFlowComputer.compute(cashFlow);
        mgRefundRow.setTotalAmount(compute.getTotalAmount());
        mgRefundRow.setMerchantAmount(compute.getMerchantAmount());
        mgRefundRow.setExternalFee(compute.getExternalFee());
        mgRefundRow.setGuaranteeDeposit(compute.getGuaranteeDeposit());
        mgRefundRow.setProviderFee(compute.getProviderFee());
        mgRefundRow.setSystemFee(compute.getSystemFee());
        mgRefundRow.setAccountId(compute.getAccountId());
    }

    private static void initContactInfo(MgRefundRow mgRefundRow, ContactInfo contactInfo) {
        if (contactInfo != null) {
            mgRefundRow.setEmail(contactInfo.getEmail());
        }
    }

    private static void initCardData(MgRefundRow mgRefundRow, PaymentTool paymentTool) {
        if (paymentTool.isSetBankCard()) {
            BankCard bankCard = paymentTool.getBankCard();
            mgRefundRow.setProvider(bankCard.getBankName());
        }
    }

}
