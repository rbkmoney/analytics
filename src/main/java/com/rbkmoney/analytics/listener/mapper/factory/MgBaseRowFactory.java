package com.rbkmoney.analytics.listener.mapper.factory;

import com.rbkmoney.analytics.computer.CashFlowComputer;
import com.rbkmoney.analytics.dao.model.MgBaseRow;
import com.rbkmoney.analytics.domain.CashFlowResult;
import com.rbkmoney.analytics.utils.TimeUtils;
import com.rbkmoney.damsel.domain.*;
import com.rbkmoney.damsel.payment_processing.InvoicePayment;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.CollectionUtils;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;

import static java.time.ZoneOffset.UTC;

@Slf4j
public abstract class MgBaseRowFactory<T extends MgBaseRow> implements RowFactory<T> {

    @Override
    public void initBaseRow(MachineEvent machineEvent, T row, InvoicePayment invoicePayment) {
        initTime(machineEvent, row);
        row.setCurrency(invoicePayment.getPayment().getCost().getCurrency().getSymbolicCode());
        row.setPaymentId(invoicePayment.getPayment().getId());
        Payer payer = invoicePayment.getPayment().getPayer();
        if (payer.isSetPaymentResource()) {
            DisposablePaymentResource resource = payer.getPaymentResource().getResource();
            PaymentTool paymentTool = resource.getPaymentTool();
            if (payer.getPaymentResource().isSetResource()) {
                if (resource.isSetClientInfo()) {
                    ClientInfo clientInfo = resource.getClientInfo();
                    row.setIp(clientInfo.getIpAddress());
                    row.setFingerprint(clientInfo.getFingerprint());
                }
                initCardData(row, paymentTool);
            }
            initContactInfo(row, payer.getPaymentResource().getContactInfo());
        } else if (payer.isSetCustomer()) {
            CustomerPayer customer = payer.getCustomer();
            PaymentTool paymentTool = customer.getPaymentTool();
            initContactInfo(row, customer.getContactInfo());
            initCardData(row, paymentTool);
        } else if (payer.isSetRecurrent()) {
            RecurrentPayer recurrent = payer.getRecurrent();
            PaymentTool paymentTool = recurrent.getPaymentTool();
            initCardData(row, paymentTool);
            initContactInfo(row, recurrent.getContactInfo());
        } else {
            log.warn("Unkonwn payment tool in payer: {}", payer);
        }
    }

    protected void initCashFlowInfo(T row, List<FinalCashFlowPosting> cashFlow) {
        if (!CollectionUtils.isEmpty(cashFlow)) {
            CashFlowResult compute = CashFlowComputer.compute(cashFlow);
            row.setTotalAmount(compute.getTotalAmount());
            row.setMerchantAmount(compute.getMerchantAmount());
            row.setExternalFee(compute.getExternalFee());
            row.setGuaranteeDeposit(compute.getGuaranteeDeposit());
            row.setProviderFee(compute.getProviderFee());
            row.setSystemFee(compute.getSystemFee());
            row.setAccountId(compute.getAccountId());
        }
    }

    private void initContactInfo(T mgRefundRow, ContactInfo contactInfo) {
        if (contactInfo != null) {
            mgRefundRow.setEmail(contactInfo.getEmail());
        }
    }

    private void initCardData(T mgRefundRow, PaymentTool paymentTool) {
        if (paymentTool.isSetBankCard()) {
            BankCard bankCard = paymentTool.getBankCard();
            mgRefundRow.setProvider(bankCard.getBankName());
        }
    }

    private void initTime(MachineEvent event, T row) {
        LocalDateTime localDateTime = TypeUtil.stringToLocalDateTime(event.getCreatedAt());
        long timestamp = localDateTime.atZone(ZoneOffset.UTC).toInstant().toEpochMilli();
        row.setTimestamp(java.sql.Date.valueOf(
                Instant.ofEpochMilli(timestamp)
                        .atZone(UTC)
                        .toLocalDate())
        );
        row.setEventTime(timestamp);
        row.setEventTimeHour(TimeUtils.parseEventTimeHour(timestamp));
    }

}
