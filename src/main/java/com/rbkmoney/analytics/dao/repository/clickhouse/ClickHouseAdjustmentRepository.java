package com.rbkmoney.analytics.dao.repository.clickhouse;

import com.rbkmoney.analytics.dao.model.MgAdjustmentRow;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;
import ru.yandex.clickhouse.except.ClickHouseException;

import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class ClickHouseAdjustmentRepository {

    private final JdbcTemplate clickHouseJdbcTemplate;

    @Retryable(value = ClickHouseException.class, backoff = @Backoff(delay = 5000))
    public void insertBatch(List<MgAdjustmentRow> adjustmentRows) {
        if (adjustmentRows != null && !adjustmentRows.isEmpty()) {
            log.info("Batch start insert adjustmentRows: {} firstElement: {}", adjustmentRows.size(),
                    adjustmentRows.get(0).getInvoiceId());
            clickHouseJdbcTemplate.batchUpdate(ClickHouseAdjustmentBatchPreparedStatementSetter.INSERT,
                    new ClickHouseAdjustmentBatchPreparedStatementSetter(adjustmentRows));
            log.info("Batch inserted adjustmentRows: {} firstElement: {}", adjustmentRows.size(),
                    adjustmentRows.get(0).getInvoiceId());
        }
    }

}
