package com.rbkmoney.analytics.listener.mapper;

import com.rbkmoney.analytics.constant.EventType;
import com.rbkmoney.analytics.constant.RefundStatus;
import com.rbkmoney.analytics.dao.model.MgRefundRow;
import com.rbkmoney.analytics.exception.RefundInfoNotFoundException;
import com.rbkmoney.analytics.service.HgClientService;
import com.rbkmoney.analytics.utils.MgRefundRowUtils;
import com.rbkmoney.analytics.utils.TimeUtils;
import com.rbkmoney.damsel.domain.Failure;
import com.rbkmoney.damsel.payment_processing.InvoiceChange;
import com.rbkmoney.damsel.payment_processing.InvoicePaymentRefundChange;
import com.rbkmoney.damsel.payment_processing.InvoicePaymentRefundChangePayload;
import com.rbkmoney.damsel.payment_processing.InvoicePaymentRefundStatusChanged;
import com.rbkmoney.geck.common.util.TBaseUtil;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static java.time.ZoneOffset.UTC;

@Slf4j
@Component
@RequiredArgsConstructor
public class RefundPaymentMapper implements Mapper<InvoiceChange, MachineEvent, MgRefundRow> {

    public static final String OPERATION_TIMEOUT = "operation_timeout";

    private final HgClientService hgClientService;

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
        MgRefundRow refundRow = MgRefundRowUtils.initInvoiceInfo(invoiceInfo, refundId);
        initTime(event, refundRow);
        refundRow.setStatus(TBaseUtil.unionFieldToEnum(payload
                .getInvoicePaymentRefundStatusChanged()
                .getStatus(), RefundStatus.class));
        refundRow.setInvoiceId(event.getSourceId());
        refundRow.setPaymentId(refundId);
        refundRow.setSequenceId((event.getEventId()));

        if (invoicePaymentRefundStatusChanged.getStatus().isSetFailed()) {
            if (invoicePaymentRefundStatusChanged.getStatus().getFailed().getFailure().isSetFailure()) {
                Failure failure = invoicePaymentRefundStatusChanged.getStatus().getFailed().getFailure().getFailure();
                refundRow.setErrorCode(failure.getCode());
            } else if (invoicePaymentRefundStatusChanged.getStatus().getFailed().getFailure().isSetOperationTimeout()) {
                refundRow.setErrorCode(OPERATION_TIMEOUT);
            }
        }

        log.debug("RefundPaymentMapper refundRow: {}", refundRow);
        return refundRow;
    }

    private void initTime(MachineEvent event, MgRefundRow refundRow) {
        LocalDateTime localDateTime = TypeUtil.stringToLocalDateTime(event.getCreatedAt());
        long timestamp = localDateTime.atZone(ZoneOffset.UTC).toInstant().toEpochMilli();
        refundRow.setTimestamp(java.sql.Date.valueOf(
                Instant.ofEpochMilli(timestamp)
                        .atZone(UTC)
                        .toLocalDate())
        );
        refundRow.setEventTime(timestamp);
        refundRow.setEventTimeHour(TimeUtils.parseEventTimeHour(timestamp));
    }

    @Override
    public EventType getChangeType() {
        return EventType.INVOICE_PAYMENT_REFUND_STATUS_CHANGED;
    }

}
