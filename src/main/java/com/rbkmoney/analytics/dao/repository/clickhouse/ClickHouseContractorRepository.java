package com.rbkmoney.analytics.dao.repository.clickhouse;

import com.rbkmoney.analytics.dao.model.ContractorRow;
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
public class ClickHouseContractorRepository {

    private final JdbcTemplate clickHouseJdbcTemplate;

    @Retryable(value = ClickHouseException.class, backoff = @Backoff(delay = 5000))
    public void insertBatch(List<ContractorRow> contractorRows) {
        if (CollectionUtils.isEmpty(contractorRows)) return;

        clickHouseJdbcTemplate.batchUpdate(
                ClickHouseContractorBatchPreparedStatementSetter.INSERT,
                new ClickHouseContractorBatchPreparedStatementSetter(contractorRows));

        log.info("Batch inserted contractorRows: {} firstElement: {}", contractorRows.size(),
                contractorRows.get(0).getPartyId());
    }

}
