package com.rbkmoney.analytics.listener.mapper.factory;

import com.rbkmoney.analytics.computer.CashFlowComputer;
import com.rbkmoney.analytics.dao.model.MgRefundRow;
import com.rbkmoney.analytics.domain.InvoicePaymentWrapper;
import com.rbkmoney.analytics.exception.AdjustmentInfoNotFoundException;
import com.rbkmoney.analytics.service.GeoProvider;
import com.rbkmoney.damsel.domain.FinalCashFlowPosting;
import com.rbkmoney.damsel.domain.Invoice;
import com.rbkmoney.damsel.payment_processing.InvoicePayment;
import com.rbkmoney.damsel.payment_processing.InvoicePaymentRefund;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;

@Slf4j
@Service
public class MgRefundRowFactory extends MgBaseRowFactory<MgRefundRow> {

    private final CashFlowComputer cashFlowComputer;

    public MgRefundRowFactory(GeoProvider geoProvider, CashFlowComputer cashFlowComputer) {
        super(geoProvider);
        this.cashFlowComputer = cashFlowComputer;
    }

    @Override
    public MgRefundRow create(MachineEvent machineEvent, InvoicePaymentWrapper invoicePaymentWrapper, String refundId) {
        MgRefundRow mgPaymentSinkRow = new MgRefundRow();
        Invoice invoice = invoicePaymentWrapper.getInvoice();
        mgPaymentSinkRow.setPartyId(invoice.getOwnerId());
        mgPaymentSinkRow.setShopId(invoice.getShopId());
        mgPaymentSinkRow.setInvoiceId(machineEvent.getSourceId());
        mgPaymentSinkRow.setSequenceId((machineEvent.getEventId()));
        initInfo(machineEvent, mgPaymentSinkRow, invoicePaymentWrapper.getInvoicePayment(), refundId);
        return mgPaymentSinkRow;
    }

    private void initInfo(MachineEvent machineEvent, MgRefundRow row, InvoicePayment payment, String refundId) {
        payment.getRefunds().stream()
                .filter(refund -> refund.getRefund().getId().equals(refundId))
                .findFirst()
                .ifPresentOrElse(adjustment ->
                                mapRow(machineEvent, row, payment, refundId, adjustment)
                        , () -> {
                            throw new AdjustmentInfoNotFoundException();
                        }
                );
    }

    private void mapRow(MachineEvent machineEvent, MgRefundRow row, InvoicePayment payment, String refundId, InvoicePaymentRefund refund) {
        List<FinalCashFlowPosting> cashFlow = refund.isSetCashFlow() ? refund.getCashFlow() : payment.getCashFlow();
        row.setRefundId(refundId);
        row.setPaymentId(payment.getPayment().getId());
        cashFlowComputer.compute(cashFlow)
                .ifPresent(row::setCashFlowResult);
        initBaseRow(machineEvent, row, payment);
    }

}
