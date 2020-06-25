package com.rbkmoney.analytics.listener.mapper.factory;

import com.rbkmoney.analytics.computer.CashFlowComputer;
import com.rbkmoney.analytics.constant.ChargebackCategory;
import com.rbkmoney.analytics.constant.ChargebackStage;
import com.rbkmoney.analytics.dao.model.ChargebackRow;
import com.rbkmoney.analytics.domain.InvoicePaymentWrapper;
import com.rbkmoney.analytics.exception.ChargebackInfoNotFoundException;
import com.rbkmoney.analytics.service.GeoProvider;
import com.rbkmoney.damsel.domain.FinalCashFlowPosting;
import com.rbkmoney.damsel.domain.Invoice;
import com.rbkmoney.damsel.payment_processing.InvoicePayment;
import com.rbkmoney.damsel.payment_processing.InvoicePaymentChargeback;
import com.rbkmoney.geck.common.util.TBaseUtil;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;

@Slf4j
@Service
public class ChargebackRowFactory extends InvoiceBaseRowFactory<ChargebackRow> {

    private final CashFlowComputer cashFlowComputer;

    public ChargebackRowFactory(GeoProvider geoProvider, CashFlowComputer cashFlowComputer) {
        super(geoProvider);
        this.cashFlowComputer = cashFlowComputer;
    }

    @Override
    public ChargebackRow create(MachineEvent machineEvent, InvoicePaymentWrapper invoicePaymentWrapper, String chargebackId) {
        ChargebackRow chargebackRow = new ChargebackRow();
        InvoicePayment payment = invoicePaymentWrapper.getInvoicePayment();
        Invoice invoice = invoicePaymentWrapper.getInvoice();
        payment.getChargebacks().stream()
                .filter(chargeback -> chargeback.getChargeback().getId().equals(chargebackId))
                .findFirst()
                .ifPresentOrElse(chargeback -> {
                            mapRow(machineEvent, chargebackRow, payment, invoice, chargebackId, chargeback);
                        }, () -> {
                            throw new ChargebackInfoNotFoundException();
                        }
                );
        return chargebackRow;
    }

    private void mapRow(MachineEvent machineEvent, ChargebackRow row, InvoicePayment payment, Invoice invoice, String chargebackId, InvoicePaymentChargeback chargeback) {
        List<FinalCashFlowPosting> cashFlow = chargeback.isSetCashFlow() ? chargeback.getCashFlow() : payment.getCashFlow();
        row.setChargebackId(chargebackId);
        row.setPaymentId(payment.getPayment().getId());
        row.setCashFlowResult(cashFlowComputer.compute(cashFlow));
        var invoicePaymentChargeback = chargeback.getChargeback();
        row.setChargebackCode(invoicePaymentChargeback.getReason().getCode());
        row.setCategory(TBaseUtil.unionFieldToEnum(invoicePaymentChargeback.getReason().getCategory(), ChargebackCategory.class));
        row.setStage(TBaseUtil.unionFieldToEnum(invoicePaymentChargeback.getStage(), ChargebackStage.class));
        initBaseRow(machineEvent, row, payment, invoice);
    }

}
