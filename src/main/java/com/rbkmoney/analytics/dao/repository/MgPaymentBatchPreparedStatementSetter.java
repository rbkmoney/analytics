package com.rbkmoney.analytics.dao.repository;

import com.rbkmoney.analytics.constant.ClickhouseUtilsValue;
import com.rbkmoney.analytics.dao.model.MgPaymentSinkRow;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

@RequiredArgsConstructor
public class MgPaymentBatchPreparedStatementSetter implements BatchPreparedStatementSetter {

    public static final String INSERT = "INSERT INTO analytic.events_sink " +
            "(timestamp, eventTime, eventTimeHour, partyId, shopId, email, " +
            "amount, currency, providerName, status, errorReason,  invoiceId, " +
            "paymentId, sequenceId, ip, bin, maskedPan, paymentTool)" +
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private final List<MgPaymentSinkRow> batch;

    @Override
    public void setValues(PreparedStatement ps, int i) throws SQLException {
        MgPaymentSinkRow mgPaymentSinkRow = batch.get(i);
        int l = 1;
        ps.setDate(l++, mgPaymentSinkRow.getTimestamp());
        ps.setLong(l++, mgPaymentSinkRow.getEventTime());
        ps.setLong(l++, mgPaymentSinkRow.getEventTimeHour());

        ps.setString(l++, mgPaymentSinkRow.getPartyId());
        ps.setString(l++, mgPaymentSinkRow.getShopId());

        ps.setString(l++, mgPaymentSinkRow.getEmail());

        ps.setLong(l++, mgPaymentSinkRow.getTotalAmount());
        ps.setString(l++, mgPaymentSinkRow.getCurrency());

        ps.setString(l++, mgPaymentSinkRow.getProvider());

        ps.setString(l++, mgPaymentSinkRow.getStatus().name());

        ps.setString(l++, mgPaymentSinkRow.getErrorCode());

        ps.setString(l++, mgPaymentSinkRow.getInvoiceId());
        ps.setString(l++, mgPaymentSinkRow.getPaymentId());
        ps.setLong(l++, mgPaymentSinkRow.getSequenceId());

        ps.setString(l++, mgPaymentSinkRow.getIp());
        ps.setString(l++, mgPaymentSinkRow.getBin());
        ps.setString(l++, mgPaymentSinkRow.getMaskedPan());
        ps.setString(l++, mgPaymentSinkRow.getPaymentTool() != null ? mgPaymentSinkRow.getPaymentTool().name() : ClickhouseUtilsValue.UNKNOWN);

    }

    @Override
    public int getBatchSize() {
        return batch.size();
    }
}
