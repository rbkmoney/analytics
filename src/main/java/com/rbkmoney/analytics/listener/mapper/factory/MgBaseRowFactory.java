package com.rbkmoney.analytics.listener.mapper.factory;

import com.rbkmoney.analytics.constant.ClickHouseUtilsValue;
import com.rbkmoney.analytics.constant.PaymentToolType;
import com.rbkmoney.analytics.dao.model.MgBaseRow;
import com.rbkmoney.analytics.service.GeoProvider;
import com.rbkmoney.analytics.utils.TimeUtils;
import com.rbkmoney.damsel.domain.*;
import com.rbkmoney.damsel.payment_processing.InvoicePayment;
import com.rbkmoney.geck.common.util.TBaseUtil;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TUnion;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static java.time.ZoneOffset.UTC;

@Slf4j
@RequiredArgsConstructor
public abstract class MgBaseRowFactory<T extends MgBaseRow> implements RowFactory<T> {

    public static final String UNKNOWN = "UNKNOWN";
    private final GeoProvider geoProvider;

    @Override
    public void initBaseRow(MachineEvent machineEvent, T row, InvoicePayment invoicePayment) {
        initTime(machineEvent, row);
        row.setCurrency(invoicePayment.getPayment().getCost().getCurrency().getSymbolicCode());
        row.setPaymentId(invoicePayment.getPayment().getId());
        Payer payer = invoicePayment.getPayment().getPayer();
        if (payer.isSetPaymentResource() && payer.getPaymentResource().isSetResource()) {
            DisposablePaymentResource resource = payer.getPaymentResource().getResource();
            PaymentTool paymentTool = resource.getPaymentTool();
            if (resource.isSetClientInfo()) {
                ClientInfo clientInfo = resource.getClientInfo();
                String ipAddress = clientInfo.getIpAddress();
                row.setIp(ipAddress);
                row.setPaymentCountry(geoProvider.getLocationIsoCode(ipAddress));
                row.setFingerprint(clientInfo.getFingerprint());
            }
            initCardData(row, paymentTool);
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
            log.warn("Unknown payment tool in payer: {}", payer);
        }
    }

    private void initContactInfo(T mgRefundRow, ContactInfo contactInfo) {
        if (contactInfo != null) {
            mgRefundRow.setEmail(contactInfo.getEmail());
        }
    }

    private void initCardData(T row, PaymentTool paymentTool) {
        TBaseUtil.unionFieldToEnum(paymentTool, PaymentToolType.class);
        if (paymentTool.isSetBankCard()) {
            BankCard bankCard = paymentTool.getBankCard();
            row.setBankCountry(bankCard.isSetIssuerCountry() ? bankCard.getIssuerCountry().name() : ClickHouseUtilsValue.UNKNOWN);
            row.setProvider(bankCard.getBankName());
            row.setCardToken(bankCard.getToken());
            row.setBin(bankCard.getBin());
            row.setMaskedPan(bankCard.getLastDigits());
            row.setPaymentSystem(bankCard.getPaymentSystem().name());
        } else if (paymentTool.isSetDigitalWallet()) {
            row.setDigitalWalletProvider(paymentTool.getDigitalWallet().getProvider().name());
            row.setDigitalWalletToken(paymentTool.getDigitalWallet().getToken());
        } else if (paymentTool.isSetCryptoCurrency()) {
            row.setCryptoCurrency(paymentTool.getCryptoCurrency().name());
        } else if (paymentTool.isSetMobileCommerce()) {
            row.setMobileOperator(paymentTool.getMobileCommerce().getOperator().name());
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
