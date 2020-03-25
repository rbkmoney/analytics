package com.rbkmoney.analytics.dao.repository.postgres;

import com.rbkmoney.analytics.dao.model.MgRefundRow;
import com.rbkmoney.analytics.domain.CashFlowResult;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

@RequiredArgsConstructor
public class PostgresRefundBatchPreparedStatementSetter implements BatchPreparedStatementSetter {

    private final List<MgRefundRow> batch;

    @Override
    public void setValues(PreparedStatement ps, int i) throws SQLException {
        MgRefundRow row = batch.get(i);
        int l = 1;
        ps.setString(l++, row.getInvoiceId() + "-" + row.getSequenceId());
        ps.setObject(l++, row.getEventTime());
        ps.setString(l++, row.getPartyId());
        ps.setString(l++, row.getShopId());

        CashFlowResult cashFlowResult = row.getCashFlowResult();
        ps.setLong(l++, -cashFlowResult.getAmount() - cashFlowResult.getSystemFee());

        ps.setString(l, row.getCurrency());
    }

    @Override
    public int getBatchSize() {
        return batch.size();
    }
}
