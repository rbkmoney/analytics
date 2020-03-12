package com.rbkmoney.analytics.listener.mapper;

import com.rbkmoney.analytics.constant.EventType;
import com.rbkmoney.analytics.constant.RefundStatus;
import com.rbkmoney.analytics.dao.model.MgRefundRow;
import com.rbkmoney.analytics.exception.RefundInfoNotFoundException;
import com.rbkmoney.analytics.listener.mapper.factory.RowFactory;
import com.rbkmoney.analytics.service.HgClientService;
import com.rbkmoney.damsel.domain.Failure;
import com.rbkmoney.damsel.payment_processing.InvoiceChange;
import com.rbkmoney.damsel.payment_processing.InvoicePaymentRefundChange;
import com.rbkmoney.damsel.payment_processing.InvoicePaymentRefundChangePayload;
import com.rbkmoney.damsel.payment_processing.InvoicePaymentRefundStatusChanged;
import com.rbkmoney.geck.common.util.TBaseUtil;
import com.rbkmoney.geck.serializer.kit.tbase.TErrorUtil;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class RefundPaymentMapper implements Mapper<InvoiceChange, MachineEvent, MgRefundRow> {

    public static final String OPERATION_TIMEOUT = "operation_timeout";

    private final HgClientService hgClientService;
    private final RowFactory<MgRefundRow> mgRefundRowFactory;

    @Override
    public MgRefundRow map(InvoiceChange change, MachineEvent event) {
        com.rbkmoney.damsel.payment_processing.Invoice invoiceInfo = hgClientService.getInvoiceInfo(event);

        if (invoiceInfo == null) {
            throw new RefundInfoNotFoundException("Not found refund info in hg!");
        }

        InvoicePaymentRefundChange invoicePaymentChange = change.getInvoicePaymentChange()
                .getPayload()
                .getInvoicePaymentRefundChange();
        InvoicePaymentRefundChangePayload payload = invoicePaymentChange.getPayload();
        InvoicePaymentRefundStatusChanged invoicePaymentRefundStatusChanged = payload.getInvoicePaymentRefundStatusChanged();
        String refundId = invoicePaymentChange.getId();

        MgRefundRow refundRow = mgRefundRowFactory.create(event, invoiceInfo, refundId);

        refundRow.setStatus(TBaseUtil.unionFieldToEnum(payload
                .getInvoicePaymentRefundStatusChanged()
                .getStatus(), RefundStatus.class));

        if (invoicePaymentRefundStatusChanged.getStatus().isSetFailed()) {
            if (invoicePaymentRefundStatusChanged.getStatus().getFailed().getFailure().isSetFailure()) {
                Failure failure = invoicePaymentRefundStatusChanged.getStatus().getFailed().getFailure().getFailure();
                refundRow.setErrorCode(TErrorUtil.toStringVal(failure));
                refundRow.setErrorReason(failure.getReason());
            } else if (invoicePaymentRefundStatusChanged.getStatus().getFailed().getFailure().isSetOperationTimeout()) {
                refundRow.setErrorCode(OPERATION_TIMEOUT);
            }
        }

        log.debug("RefundPaymentMapper refundRow: {}", refundRow);
        return refundRow;
    }

    @Override
    public EventType getChangeType() {
        return EventType.INVOICE_PAYMENT_REFUND_STATUS_CHANGED;
    }

}
