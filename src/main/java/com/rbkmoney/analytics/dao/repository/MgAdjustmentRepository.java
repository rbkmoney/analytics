package com.rbkmoney.analytics.dao.repository;

import com.rbkmoney.analytics.dao.model.MgAdjustmentRow;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class MgAdjustmentRepository {

    private final JdbcTemplate jdbcTemplate;

    public void insertBatch(List<MgAdjustmentRow> adjustmentRows) {
        if (adjustmentRows != null && !adjustmentRows.isEmpty()) {
            log.info("Batch start insert adjustmentRows: {} firstElement: {}", adjustmentRows.size(),
                    adjustmentRows.get(0).getInvoiceId());
            jdbcTemplate.batchUpdate(MgPaymentBatchPreparedStatementSetter.INSERT,
                    new MgAdjustmentBatchPreparedStatementSetter(adjustmentRows));
            log.info("Batch inserted adjustmentRows: {} firstElement: {}", adjustmentRows.size(),
                    adjustmentRows.get(0).getInvoiceId());
        }
    }

}
