package com.rbkmoney.analytics.dao.repository.clickhouse;

import com.rbkmoney.analytics.dao.model.MgAdjustmentRow;
import com.rbkmoney.analytics.dao.model.MgChargebackRow;
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
public class ClickHouseChargebackRepository {

    private final JdbcTemplate clickHouseJdbcTemplate;

    @Retryable(value = ClickHouseException.class, backoff = @Backoff(delay = 5000))
    public void insertBatch(List<MgChargebackRow> mgChargebackRows) {
        if (mgChargebackRows != null && !mgChargebackRows.isEmpty()) {
            log.info("Batch start insert mgChargebackRows: {} firstElement: {}", mgChargebackRows.size(),
                    mgChargebackRows.get(0).getInvoiceId());
            clickHouseJdbcTemplate.batchUpdate(ClickHouseChargebackBatchPreparedStatementSetter.INSERT,
                    new ClickHouseChargebackBatchPreparedStatementSetter(mgChargebackRows));
            log.info("Batch inserted mgChargebackRows: {} firstElement: {}", mgChargebackRows.size(),
                    mgChargebackRows.get(0).getInvoiceId());
        }
    }

}
