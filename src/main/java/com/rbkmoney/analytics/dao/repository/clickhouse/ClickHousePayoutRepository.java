package com.rbkmoney.analytics.dao.repository.clickhouse;

import com.rbkmoney.analytics.dao.model.PayoutRow;
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
public class ClickHousePayoutRepository {

    private final JdbcTemplate clickHouseJdbcTemplate;

    @Retryable(value = ClickHouseException.class, backoff = @Backoff(delay = 5000))
    public void insertBatch(List<PayoutRow> payoutRows) {
        if (payoutRows != null && !payoutRows.isEmpty()) {
            // TODO [a.romanov]: impl
            throw new UnsupportedOperationException();
        }
    }

}