package com.rbkmoney.analytics.dao.repository;

import com.rbkmoney.analytics.dao.model.MgRefundRow;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

@RequiredArgsConstructor
public class MgRefundBatchPreparedStatementSetter implements BatchPreparedStatementSetter {

    public static final String INSERT = "INSERT INTO analytic.events_sink_refund " +
            "(timestamp, eventTime, eventTimeHour, partyId, shopId, email, " +
            "totalAmount, merchantAmount, guaranteeDeposit, systemFee, providerFee, externalFee, currency, providerName, " +
            "status, errorReason,  invoiceId, " +
            "paymentId, refundId, sequenceId, ip)" +
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private final List<MgRefundRow> batch;

    @Override
    public void setValues(PreparedStatement ps, int i) throws SQLException {
        MgRefundRow mgRefundRow = batch.get(i);
        int l = 1;
        ps.setDate(l++, mgRefundRow.getTimestamp());
        ps.setLong(l++, mgRefundRow.getEventTime());
        ps.setLong(l++, mgRefundRow.getEventTimeHour());

        ps.setString(l++, mgRefundRow.getPartyId());
        ps.setString(l++, mgRefundRow.getShopId());

        ps.setString(l++, mgRefundRow.getEmail());

        ps.setLong(l++, mgRefundRow.getTotalAmount());
        ps.setLong(l++, mgRefundRow.getMerchantAmount());
        ps.setLong(l++, mgRefundRow.getGuaranteeDeposit());
        ps.setLong(l++, mgRefundRow.getSystemFee());
        ps.setLong(l++, mgRefundRow.getExternalFee());
        ps.setLong(l++, mgRefundRow.getProviderFee());
        ps.setString(l++, mgRefundRow.getCurrency());

        ps.setString(l++, mgRefundRow.getProvider());

        ps.setString(l++, mgRefundRow.getStatus().name());

        ps.setString(l++, mgRefundRow.getErrorCode());

        ps.setString(l++, mgRefundRow.getInvoiceId());
        ps.setString(l++, mgRefundRow.getPaymentId());
        ps.setString(l++, mgRefundRow.getRefundId());
        ps.setLong(l++, mgRefundRow.getSequenceId());

        ps.setString(l, mgRefundRow.getIp());
    }

    @Override
    public int getBatchSize() {
        return batch.size();
    }
}
