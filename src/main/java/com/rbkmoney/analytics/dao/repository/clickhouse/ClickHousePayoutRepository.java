package com.rbkmoney.analytics.dao.repository.clickhouse;

import com.rbkmoney.analytics.dao.model.NumberModel;
import com.rbkmoney.analytics.dao.model.PayoutRow;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import ru.yandex.clickhouse.except.ClickHouseException;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class ClickHousePayoutRepository {

    private final JdbcTemplate clickHouseJdbcTemplate;

    @Retryable(value = ClickHouseException.class, backoff = @Backoff(delay = 5000))
    public void insertBatch(List<PayoutRow> payoutRows) {
        if (CollectionUtils.isEmpty(payoutRows)) {
            return;
        }

        clickHouseJdbcTemplate.batchUpdate(
                ClickHousePayoutBatchPreparedStatementSetter.INSERT,
                new ClickHousePayoutBatchPreparedStatementSetter(payoutRows));

        log.info("Batch inserted payoutRows: {} firstElement: {}", payoutRows.size(),
                payoutRows.get(0).getPayoutId());
    }

    public String getPaidEvent(String payoutId) {
        String selectSql = "select payoutId from analytic.events_sink_payout where payoutId = ? and status = 'paid'";
        try {
            return clickHouseJdbcTemplate.queryForObject(selectSql, String.class, payoutId);
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }
}
