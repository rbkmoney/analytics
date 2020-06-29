package com.rbkmoney.analytics.dao.repository.postgres;

import com.rbkmoney.analytics.constant.PayoutStatus;
import com.rbkmoney.analytics.dao.model.PayoutRow;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

@RequiredArgsConstructor
public class PostgresPayoutBatchPreparedStatementSetter implements BatchPreparedStatementSetter {

    private final List<PayoutRow> batch;

    @Override
    public void setValues(PreparedStatement ps, int i) throws SQLException {
        PayoutRow row = batch.get(i);
        int l = 1;
        ps.setString(l++, row.getPayoutId());
        ps.setObject(l++, row.getEventTime());
        ps.setString(l++, row.getPartyId());
        ps.setString(l++, row.getShopId());

        if (row.getStatus() == PayoutStatus.cancelled && row.isCancelledAfterBeingPaid()) {
            ps.setLong(l++, row.getAmount() - row.getFee());
        } else {
            ps.setLong(l++, -row.getAmount() - row.getFee());
        }

        ps.setString(l, row.getCurrency());
    }

    @Override
    public int getBatchSize() {
        return batch.size();
    }
}
