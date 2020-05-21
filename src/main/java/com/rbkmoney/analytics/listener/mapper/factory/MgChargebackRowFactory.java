package com.rbkmoney.analytics.listener.mapper.factory;

import com.rbkmoney.analytics.computer.CashFlowComputer;
import com.rbkmoney.analytics.constant.ChargebackCategory;
import com.rbkmoney.analytics.constant.ChargebackStage;
import com.rbkmoney.analytics.dao.model.MgChargebackRow;
import com.rbkmoney.analytics.domain.InvoicePaymentWrapper;
import com.rbkmoney.analytics.exception.AdjustmentInfoNotFoundException;
import com.rbkmoney.analytics.service.GeoProvider;
import com.rbkmoney.damsel.domain.FinalCashFlowPosting;
import com.rbkmoney.damsel.domain.Invoice;
import com.rbkmoney.damsel.domain.InvoicePaymentChargeback;
import com.rbkmoney.damsel.payment_processing.InvoicePayment;
import com.rbkmoney.damsel.payment_processing.InvoicePaymentRefund;
import com.rbkmoney.geck.common.util.TBaseUtil;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

@Slf4j
@Service
public class MgChargebackRowFactory extends MgBaseRowFactory<MgChargebackRow> {

    private final CashFlowComputer cashFlowComputer;

    public MgChargebackRowFactory(GeoProvider geoProvider, CashFlowComputer cashFlowComputer) {
        super(geoProvider);
        this.cashFlowComputer = cashFlowComputer;
    }

    @Override
    public MgChargebackRow create(MachineEvent machineEvent, InvoicePaymentWrapper invoicePaymentWrapper, String chargebackId) {
        MgChargebackRow mgChargebackRow = new MgChargebackRow();
        Invoice invoice = invoicePaymentWrapper.getInvoice();
        mgChargebackRow.setPartyId(invoice.getOwnerId());
        mgChargebackRow.setShopId(invoice.getShopId());
        mgChargebackRow.setInvoiceId(machineEvent.getSourceId());
        mgChargebackRow.setSequenceId((machineEvent.getEventId()));
        initInfo(machineEvent, mgChargebackRow, invoicePaymentWrapper.getInvoicePayment(), chargebackId);
        return mgChargebackRow;
    }

    private void initInfo(MachineEvent machineEvent, MgChargebackRow row, InvoicePayment payment, String chargebackId) {
        payment.getChargebacks().stream()
                .filter(chargeback -> chargeback.getId().equals(chargebackId))
                .findFirst()
                .ifPresentOrElse(adjustment -> mapRow(machineEvent, row, payment, chargebackId, adjustment), () -> {
                            throw new AdjustmentInfoNotFoundException();
                        }
                );
    }

    private void mapRow(MachineEvent machineEvent, MgChargebackRow row, InvoicePayment payment, String chargebackId, InvoicePaymentChargeback chargeback) {
//        List<FinalCashFlowPosting> cashFlow = chargeback.isSetBody() ? chargeback.getCashFlow() : payment.getCashFlow();
        row.setChargebackId(chargebackId);
        row.setPaymentId(payment.getPayment().getId());
        //TODO
//        row.setCashFlowResult(cashFlowComputer.compute(cashFlow));
        row.setChargebackCode(chargeback.getReason().getCode());
        row.setCategory(TBaseUtil.unionFieldToEnum(chargeback.getReason().getCategory(), ChargebackCategory.class));
        row.setStage(TBaseUtil.unionFieldToEnum(chargeback.getStage(), ChargebackStage.class));
        initBaseRow(machineEvent, row, payment);
    }

}
