package com.rbkmoney.analytics.dao.repository.clickhouse;

import com.rbkmoney.analytics.dao.model.PartyRow;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import ru.yandex.clickhouse.except.ClickHouseException;

import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class ClickHousePartyRepository {

    private final JdbcTemplate clickHouseJdbcTemplate;

    private final RowMapper<PartyRow> partyRowRowMapper;

    @Retryable(value = ClickHouseException.class, backoff = @Backoff(delay = 5000))
    public void insertBatch(List<PartyRow> partyRows) {
        if (CollectionUtils.isEmpty(partyRows)) return;

        clickHouseJdbcTemplate.batchUpdate(
                ClickHousePartyBatchPreparedStatementSetter.INSERT,
                new ClickHousePartyBatchPreparedStatementSetter(partyRows));

        log.info("Batch inserted partyRows: {} firstElement: {}", partyRows.size(),
                partyRows.get(0).getPartyId());
    }

    public PartyRow getParty(String partyId) {
//        return clickHouseJdbcTemplate.queryForObject("SELECT eventTime, partyId, createdAt, email, blocking," +
//                " blockedReason, blockedSince, unblockedReason, unblockedSince, suspension, suspensionActiveSince," +
//                " suspensionSuspendedSince, revisionId, revisionChangedAt" +
//                " FROM analytic.events_sink_party" +
//                " WHERE (timestamp = (SELECT max(timestamp) FROM analytic.events_sink_party WHERE partyId = ?))",
//                new Object[] { partyId }, partyRowRowMapper);
        return new PartyRow();
    }

}
