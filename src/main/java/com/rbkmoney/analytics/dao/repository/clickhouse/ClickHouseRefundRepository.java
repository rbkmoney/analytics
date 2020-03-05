package com.rbkmoney.analytics.dao.repository.clickhouse;

import com.rbkmoney.analytics.dao.mapper.CommonRowsMapper;
import com.rbkmoney.analytics.dao.model.NumberModel;
import com.rbkmoney.analytics.dao.model.MgRefundRow;
import com.rbkmoney.analytics.dao.utils.DateFilterUtils;
import com.rbkmoney.analytics.dao.utils.QueryUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import ru.yandex.clickhouse.except.ClickHouseException;

import java.sql.Date;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class ClickHouseRefundRepository {

    private final JdbcTemplate clickHouseJdbcTemplate;
    private final CommonRowsMapper<NumberModel> costCommonRowsMapper;

    @Retryable(value = ClickHouseException.class, backoff = @Backoff(delay = 5000))
    public void insertBatch(List<MgRefundRow> mgRefundRows) {
        if (mgRefundRows != null && !mgRefundRows.isEmpty()) {
            clickHouseJdbcTemplate.batchUpdate(ClickHouseRefundBatchPreparedStatementSetter.INSERT, new ClickHouseRefundBatchPreparedStatementSetter(mgRefundRows));
            log.info("Batch inserted mgRefundRows: {} firstElement: {}", mgRefundRows.size(),
                    mgRefundRows.get(0).getInvoiceId());
        }
    }

    public List<NumberModel> getPaymentsAmount(String partyId,
                                               List<String> shopIds,
                                               Long from,
                                               Long to) {
        Date dateFrom = DateFilterUtils.parseDate(from);
        Date dateTo = DateFilterUtils.parseDate(to);

        String selectSql = "SELECT currency, sum(amount * sign) as amount " +
                "from analytic.events_sink_refund ";
        String whereSql = "where timestamp >= ? and timestamp <= ? AND eventTimeHour >= ? AND eventTimeHour <= ? AND eventTime >= ? AND eventTime <= ?";
        String groupedSql = " group by partyId, currency " +
                " having partyId = ? " +
                " AND sum(sign) > 0";

        String sql = selectSql;
        List<Object> params = null;

        if (!CollectionUtils.isEmpty(shopIds)) {
            StringBuilder inList = QueryUtils.generateInList(shopIds);
            sql = sql + whereSql + " AND shopId " + inList + groupedSql;
            params = new ArrayList<>(Arrays.asList(dateFrom, dateTo, from, to, from, to));
            params.addAll(shopIds);
            params.add(partyId);
        } else {
            sql = sql + whereSql + groupedSql;
            params = new ArrayList<>(Arrays.asList(dateFrom, dateTo, from, to, from, to, partyId));
        }

        List<Map<String, Object>> rows = clickHouseJdbcTemplate.queryForList(sql, params.toArray());
        return costCommonRowsMapper.map(rows);
    }

}
