package com.rbkmoney.analytics.listener;

import com.rbkmoney.analytics.computer.CashFlowComputer;
import com.rbkmoney.analytics.constant.ClickhouseUtilsValue;
import com.rbkmoney.analytics.constant.PaymentStatus;
import com.rbkmoney.analytics.constant.PaymentToolType;
import com.rbkmoney.analytics.dao.model.MgPaymentSinkRow;
import com.rbkmoney.analytics.domain.CashFlowResult;
import com.rbkmoney.analytics.exception.PaymentInfoNotFoundException;
import com.rbkmoney.analytics.exception.PaymentInfoRequestException;
import com.rbkmoney.analytics.utils.TimeUtils;
import com.rbkmoney.damsel.domain.Invoice;
import com.rbkmoney.damsel.domain.*;
import com.rbkmoney.damsel.payment_processing.InvoicePayment;
import com.rbkmoney.damsel.payment_processing.*;
import com.rbkmoney.geck.common.util.TBaseUtil;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;

import static java.time.ZoneOffset.UTC;

@Slf4j
@Component
@RequiredArgsConstructor
public class InvoicePaymentStatusChangedHandlerImpl implements EventHandler<MgPaymentSinkRow> {

    public static final String OPERATION_TIMEOUT = "operation_timeout";
    private final InvoicingSrv.Iface invoicingClient;
    private final CashFlowComputer cashFlowComputer;

    private final UserInfo userInfo = new UserInfo("analytics", UserType.service_user(new ServiceUser()));

    @Override
    public MgPaymentSinkRow handle(InvoiceChange change, MachineEvent event) {
        com.rbkmoney.damsel.payment_processing.Invoice invoiceInfo = null;
        try {
            invoiceInfo = invoicingClient.get(userInfo, event.getSourceId(),
                    new EventRange().setLimit((int) event.getEventId()));
        } catch (TException e) {
            throw new PaymentInfoRequestException(e);
        }

        InvoicePaymentChange invoicePaymentChange = change.getInvoicePaymentChange();
        InvoicePaymentChangePayload payload = invoicePaymentChange.getPayload();
        InvoicePaymentStatusChanged invoicePaymentStatusChanged = payload.getInvoicePaymentStatusChanged();

        String paymentId = change.getInvoicePaymentChange().getId();

        MgPaymentSinkRow mgPaymentSinkRow = new MgPaymentSinkRow();
        mgPaymentSinkRow.setInvoiceId(event.getSourceId());
        mgPaymentSinkRow.setStatus(TBaseUtil.unionFieldToEnum(invoicePaymentStatusChanged.getStatus(), PaymentStatus.class));
        mgPaymentSinkRow.setPaymentId(paymentId);
        mgPaymentSinkRow.setSequenceId((event.getEventId()));

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

        if (invoicePaymentStatusChanged.getStatus().isSetFailed()) {
            if (invoicePaymentStatusChanged.getStatus().getFailed().getFailure().isSetFailure()) {
                Failure failure = invoicePaymentStatusChanged.getStatus().getFailed().getFailure().getFailure();
                mgPaymentSinkRow.setErrorCode(failure.getCode());
            } else if (invoicePaymentStatusChanged.getStatus().getFailed().getFailure().isSetOperationTimeout()) {
                mgPaymentSinkRow.setErrorCode(OPERATION_TIMEOUT);
            }
        }

        return mgPaymentSinkRow;
    }

    private void initPaymentInfo(MgPaymentSinkRow mgPaymentSinkRow, InvoicePayment invoicePayment) {
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
        long eventTimeHour = TimeUtils.parseEventTimeHour(timestamp);
        mgPaymentSinkRow.setEventTimeHour(eventTimeHour);

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

    private void initCashFlowInfo(MgPaymentSinkRow mgPaymentSinkRow, List<FinalCashFlowPosting> cashFlow) {
        CashFlowResult compute = cashFlowComputer.compute(cashFlow);
        mgPaymentSinkRow.setTotalAmount(compute.getTotalAmount());
        mgPaymentSinkRow.setMerchantAmount(compute.getMerchantAmount());
        mgPaymentSinkRow.setExternalFee(compute.getExternalFee());
        mgPaymentSinkRow.setGuaranteeDeposit(compute.getGuaranteeDeposit());
        mgPaymentSinkRow.setProviderFee(compute.getProviderFee());
        mgPaymentSinkRow.setSystemFee(compute.getSystemFee());
        mgPaymentSinkRow.setAccountId(compute.getAccountId());
    }

    private void initContactInfo(MgPaymentSinkRow mgPaymentSinkRow, ContactInfo contactInfo) {
        if (contactInfo != null) {
            mgPaymentSinkRow.setEmail(contactInfo.getEmail());
        }
    }

    private void initPaymentTool(MgPaymentSinkRow mgPaymentSinkRow, PaymentTool paymentTool) {
        mgPaymentSinkRow.setPaymentTool(TBaseUtil.unionFieldToEnum(paymentTool, PaymentToolType.class));
    }

    private void initCardData(MgPaymentSinkRow mgPaymentSinkRow, PaymentTool paymentTool) {
        if (paymentTool.isSetBankCard()) {
            BankCard bankCard = paymentTool.getBankCard();
            mgPaymentSinkRow.setBankCountry(bankCard.getIssuerCountry() != null ? bankCard.getIssuerCountry().name() : ClickhouseUtilsValue.UNKNOWN);
            mgPaymentSinkRow.setBin(bankCard.getBin());
            mgPaymentSinkRow.setMaskedPan(bankCard.getLastDigits());
            mgPaymentSinkRow.setProvider(bankCard.getBankName());
        }
    }
}
