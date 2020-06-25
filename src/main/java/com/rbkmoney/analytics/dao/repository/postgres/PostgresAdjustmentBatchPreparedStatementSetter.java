package com.rbkmoney.analytics.dao.repository.postgres;

import com.rbkmoney.analytics.dao.model.AdjustmentRow;
import com.rbkmoney.analytics.domain.CashFlowResult;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

@RequiredArgsConstructor
public class PostgresAdjustmentBatchPreparedStatementSetter implements BatchPreparedStatementSetter {

    private final List<AdjustmentRow> batch;

    @Override
    public void setValues(PreparedStatement ps, int i) throws SQLException {
        AdjustmentRow row = batch.get(i);
        int l = 1;
        ps.setString(l++, row.getInvoiceId() + "-" + row.getSequenceId());
        ps.setObject(l++, row.getEventTime());
        ps.setString(l++, row.getPartyId());
        ps.setString(l++, row.getShopId());

        CashFlowResult cashFlowResult = row.getCashFlowResult();
        CashFlowResult reversedCashFlowResult = row.getOldCashFlowResult();
        ps.setLong(l++, reversedCashFlowResult.getSystemFee() - cashFlowResult.getSystemFee());

        ps.setString(l, row.getCurrency());
    }

    @Override
    public int getBatchSize() {
        return batch.size();
    }
}


