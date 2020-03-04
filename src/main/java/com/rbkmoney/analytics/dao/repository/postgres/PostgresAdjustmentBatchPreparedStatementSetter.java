package com.rbkmoney.analytics.dao.repository.postgres;

import com.rbkmoney.analytics.dao.model.MgAdjustmentRow;
import com.rbkmoney.analytics.domain.CashFlowResult;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

@RequiredArgsConstructor
public class PostgresAdjustmentBatchPreparedStatementSetter implements BatchPreparedStatementSetter {

    private final List<MgAdjustmentRow> batch;

    @Override
    public void setValues(PreparedStatement ps, int i) throws SQLException {
        MgAdjustmentRow row = batch.get(i);
        int l = 1;
        ps.setDate(l++, row.getTimestamp());
        ps.setString(l++, row.getPartyId());
        ps.setString(l++, row.getShopId());

        CashFlowResult cashFlowResult = row.getCashFlowResult();
        CashFlowResult reversedCashFlowResult = row.getOldCashFlowResult();
        if (cashFlowResult != null && reversedCashFlowResult != null) {
            ps.setLong(l++, reversedCashFlowResult.getSystemFee() - cashFlowResult.getSystemFee());
        }

        ps.setString(l, row.getCurrency());
    }

    @Override
    public int getBatchSize() {
        return batch.size();
    }
}


