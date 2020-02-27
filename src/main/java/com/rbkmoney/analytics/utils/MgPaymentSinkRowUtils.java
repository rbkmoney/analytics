package com.rbkmoney.analytics.utils;

import com.rbkmoney.analytics.computer.CashFlowComputer;
import com.rbkmoney.analytics.constant.ClickhouseUtilsValue;
import com.rbkmoney.analytics.constant.PaymentToolType;
import com.rbkmoney.analytics.dao.model.MgPaymentSinkRow;
import com.rbkmoney.analytics.domain.CashFlowResult;
import com.rbkmoney.analytics.exception.PaymentInfoNotFoundException;
import com.rbkmoney.damsel.domain.*;
import com.rbkmoney.damsel.payment_processing.InvoicePayment;
import com.rbkmoney.geck.common.util.TBaseUtil;
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
public class MgPaymentSinkRowUtils {

    public static MgPaymentSinkRow initInvoiceInfo(com.rbkmoney.damsel.payment_processing.Invoice invoiceInfo, String paymentId) {
        MgPaymentSinkRow mgPaymentSinkRow = new MgPaymentSinkRow();

        Invoice invoice = invoiceInfo.getInvoice();
        mgPaymentSinkRow.setPartyId(invoice.getOwnerId());
        mgPaymentSinkRow.setShopId(invoice.getShopId());

        invoiceInfo.getPayments().stream()
                .filter(invoicePayment -> invoicePayment.isSetPayment() && invoicePayment.getPayment().getId().equals(paymentId))
                .findFirst()
                .ifPresentOrElse(invoicePayment -> initPaymentInfo(mgPaymentSinkRow, invoicePayment),
                        () -> {
                            throw new PaymentInfoNotFoundException();
                        });

        return mgPaymentSinkRow;
    }

    private static void initPaymentInfo(MgPaymentSinkRow mgPaymentSinkRow, InvoicePayment invoicePayment) {
        List<FinalCashFlowPosting> cashFlow = invoicePayment.getCashFlow();
        initCashFlowInfo(mgPaymentSinkRow, cashFlow);

        com.rbkmoney.damsel.domain.InvoicePayment payment = invoicePayment.getPayment();
        LocalDateTime localDateTime = TypeUtil.stringToLocalDateTime(payment.getCreatedAt());
        long timestamp = localDateTime.atZone(ZoneOffset.UTC).toInstant().toEpochMilli();

        mgPaymentSinkRow.setTimestamp(java.sql.Date.valueOf(
                Instant.ofEpochMilli(timestamp)
                        .atZone(UTC)
                        .toLocalDate())
        );
        mgPaymentSinkRow.setEventTime(timestamp);
        mgPaymentSinkRow.setEventTimeHour(TimeUtils.parseEventTimeHour(timestamp));
        mgPaymentSinkRow.setCurrency(payment.getCost().getCurrency().getSymbolicCode());

        Payer payer = payment.getPayer();
        if (payer.isSetPaymentResource()) {
            DisposablePaymentResource resource = payer.getPaymentResource().getResource();
            PaymentTool paymentTool = resource.getPaymentTool();
            initPaymentTool(mgPaymentSinkRow, paymentTool);
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
            initPaymentTool(mgPaymentSinkRow, paymentTool);
            initContactInfo(mgPaymentSinkRow, customer.getContactInfo());
            initCardData(mgPaymentSinkRow, paymentTool);
        } else if (payer.isSetRecurrent()) {
            RecurrentPayer recurrent = payer.getRecurrent();
            PaymentTool paymentTool = recurrent.getPaymentTool();
            initPaymentTool(mgPaymentSinkRow, paymentTool);
            initCardData(mgPaymentSinkRow, paymentTool);
            initContactInfo(mgPaymentSinkRow, recurrent.getContactInfo());
        } else {
            log.warn("Unkonwn payment tool in payer: {}", payer);
        }
    }

    private static void initCashFlowInfo(MgPaymentSinkRow mgPaymentSinkRow, List<FinalCashFlowPosting> cashFlow) {
        CashFlowResult compute = CashFlowComputer.compute(cashFlow);
        mgPaymentSinkRow.setTotalAmount(compute.getTotalAmount());
        mgPaymentSinkRow.setMerchantAmount(compute.getMerchantAmount());
        mgPaymentSinkRow.setExternalFee(compute.getExternalFee());
        mgPaymentSinkRow.setGuaranteeDeposit(compute.getGuaranteeDeposit());
        mgPaymentSinkRow.setProviderFee(compute.getProviderFee());
        mgPaymentSinkRow.setSystemFee(compute.getSystemFee());
        mgPaymentSinkRow.setAccountId(compute.getAccountId());
    }

    private static void initContactInfo(MgPaymentSinkRow mgPaymentSinkRow, ContactInfo contactInfo) {
        if (contactInfo != null) {
            mgPaymentSinkRow.setEmail(contactInfo.getEmail());
        }
    }

    private static void initPaymentTool(MgPaymentSinkRow mgPaymentSinkRow, PaymentTool paymentTool) {
        mgPaymentSinkRow.setPaymentTool(TBaseUtil.unionFieldToEnum(paymentTool, PaymentToolType.class));
    }

    private static void initCardData(MgPaymentSinkRow mgPaymentSinkRow, PaymentTool paymentTool) {
        if (paymentTool.isSetBankCard()) {
            BankCard bankCard = paymentTool.getBankCard();
            mgPaymentSinkRow.setBankCountry(bankCard.getIssuerCountry() != null ? bankCard.getIssuerCountry().name() : ClickhouseUtilsValue.UNKNOWN);
            mgPaymentSinkRow.setBin(bankCard.getBin());
            mgPaymentSinkRow.setMaskedPan(bankCard.getLastDigits());
            mgPaymentSinkRow.setProvider(bankCard.getBankName());
        }
    }

}
