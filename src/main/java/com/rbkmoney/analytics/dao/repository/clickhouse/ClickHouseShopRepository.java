package com.rbkmoney.analytics.dao.repository.clickhouse;

import com.rbkmoney.analytics.dao.model.ShopRow;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import ru.yandex.clickhouse.except.ClickHouseException;

import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class ClickHouseShopRepository {

    private final JdbcTemplate clickHouseJdbcTemplate;

    @Retryable(value = ClickHouseException.class, backoff = @Backoff(delay = 5000))
    public void insertBatch(List<ShopRow> shopRows) {
        if (CollectionUtils.isEmpty(shopRows)) return;

        clickHouseJdbcTemplate.batchUpdate(
                ClickHouseShopBatchPreparedStatementSetter.INSERT,
                new ClickHouseShopBatchPreparedStatementSetter(shopRows));

        log.info("Batch inserted shopRows: {} firstElement: {}", shopRows.size(),
                shopRows.get(0).getPartyId());
    }

}
