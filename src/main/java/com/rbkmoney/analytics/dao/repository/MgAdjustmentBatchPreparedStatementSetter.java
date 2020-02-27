package com.rbkmoney.analytics.dao.repository;

import com.rbkmoney.analytics.dao.model.MgAdjustmentRow;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

@RequiredArgsConstructor
public class MgAdjustmentBatchPreparedStatementSetter implements BatchPreparedStatementSetter {

    public static final String INSERT = "INSERT INTO analytic.events_sink_adjustment " +
            "(timestamp, eventTime, eventTimeHour, partyId, shopId, email, " +
            "totalAmount, merchantAmount, guaranteeDeposit, systemFee, providerFee, externalFee, currency, providerName, " +
            "status, errorReason,  invoiceId, " +
            "paymentId, adjustmentId, sequenceId, ip)" +
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private final List<MgAdjustmentRow> batch;

    @Override
    public void setValues(PreparedStatement ps, int i) throws SQLException {
        MgAdjustmentRow mgAdjustmentRow = batch.get(i);
        int l = 1;
        ps.setDate(l++, mgAdjustmentRow.getTimestamp());
        ps.setLong(l++, mgAdjustmentRow.getEventTime());
        ps.setLong(l++, mgAdjustmentRow.getEventTimeHour());

        ps.setString(l++, mgAdjustmentRow.getPartyId());
        ps.setString(l++, mgAdjustmentRow.getShopId());

        ps.setString(l++, mgAdjustmentRow.getEmail());

        ps.setLong(l++, mgAdjustmentRow.getTotalAmount());
        ps.setLong(l++, mgAdjustmentRow.getMerchantAmount());
        ps.setLong(l++, mgAdjustmentRow.getGuaranteeDeposit());
        ps.setLong(l++, mgAdjustmentRow.getSystemFee());
        ps.setLong(l++, mgAdjustmentRow.getExternalFee());
        ps.setLong(l++, mgAdjustmentRow.getProviderFee());
        ps.setString(l++, mgAdjustmentRow.getCurrency());

        ps.setString(l++, mgAdjustmentRow.getProvider());

        ps.setString(l++, mgAdjustmentRow.getStatus().name());

        ps.setString(l++, mgAdjustmentRow.getErrorCode());

        ps.setString(l++, mgAdjustmentRow.getInvoiceId());
        ps.setString(l++, mgAdjustmentRow.getPaymentId());
        ps.setString(l++, mgAdjustmentRow.getAdjustmentId());
        ps.setLong(l++, mgAdjustmentRow.getSequenceId());

        ps.setString(l, mgAdjustmentRow.getIp());
    }

    @Override
    public int getBatchSize() {
        return batch.size();
    }
}
