package com.rbkmoney.analytics.dao.repository.clickhouse;

import com.rbkmoney.analytics.dao.mapper.CommonRowsMapper;
import com.rbkmoney.analytics.dao.mapper.SplitRowsMapper;
import com.rbkmoney.analytics.dao.mapper.SplitStatusRowsMapper;
import com.rbkmoney.analytics.dao.model.*;
import com.rbkmoney.analytics.dao.utils.QueryUtils;
import com.rbkmoney.analytics.dao.utils.SplitUtils;
import com.rbkmoney.damsel.analytics.SplitUnit;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import ru.yandex.clickhouse.except.ClickHouseException;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class ClickHousePaymentRepository {

    public static final String SHOP_ID = "shopId";
    public static final String PARTY_ID = "partyId";
    public static final String PAYMENT_TOOL = "paymentTool";
    public static final String ERROR_REASON = "errorReason";
    public static final String WHERE_TIME_PARAMS = "where timestamp >= ? and timestamp <= ? AND eventTimeHour >= ? AND eventTimeHour <= ? AND eventTime >= ? AND eventTime <= ?";

    private final JdbcTemplate clickHouseJdbcTemplate;

    private final CommonRowsMapper<NumberModel> costCommonRowsMapper;
    private final CommonRowsMapper<NumberModel> countModelCommonRowsMapper;
    private final CommonRowsMapper<NamingDistribution> namingDistributionCommonRowsMapper;
    private final SplitRowsMapper splitCostCommonRowsMapper;
    private final SplitStatusRowsMapper splitStatusRowsMapper;

    @Retryable(value = ClickHouseException.class, backoff = @Backoff(delay = 5000))
    public void insertBatch(List<MgPaymentSinkRow> mgPaymentSinkRows) {
        if (mgPaymentSinkRows != null && !mgPaymentSinkRows.isEmpty()) {
            log.info("Batch start insert mgPaymentSinkRows: {} firstElement: {}", mgPaymentSinkRows.size(),
                    mgPaymentSinkRows.get(0).getInvoiceId());
            clickHouseJdbcTemplate.batchUpdate(ClickHousePaymentBatchPreparedStatementSetter.INSERT,
                    new ClickHousePaymentBatchPreparedStatementSetter(mgPaymentSinkRows));
            log.info("Batch inserted mgPaymentSinkRows: {} firstElement: {}", mgPaymentSinkRows.size(),
                    mgPaymentSinkRows.get(0).getInvoiceId());
        }
    }

    public List<NumberModel> getAveragePayment(String partyId,
                                               List<String> shopIds,
                                               LocalDateTime from,
                                               LocalDateTime to) {
        String selectSql = "SELECT currency, avg(amount) as num from analytic.events_sink ";
        String groupedSql = " group by partyId, currency having partyId = ? ";

        List<Map<String, Object>> rows = splitQuery(partyId, shopIds, from, to, WHERE_TIME_PARAMS, groupedSql, selectSql);
        return costCommonRowsMapper.map(rows);
    }

    public List<NumberModel> getPaymentsAmount(String partyId,
                                               List<String> shopIds,
                                               LocalDateTime from,
                                               LocalDateTime to) {
        String selectSql = "SELECT currency, sum(amount) as num from analytic.events_sink ";
        String whereSql = "where timestamp >= ? and timestamp <= ? AND eventTimeHour >= ? AND eventTimeHour <= ? AND eventTime >= ? AND eventTime <= ? and status='captured'";
        String groupedSql = " group by partyId, currency having partyId = ? ";

        List<Map<String, Object>> rows = splitQuery(partyId, shopIds, from, to, whereSql, groupedSql, selectSql);
        return costCommonRowsMapper.map(rows);
    }

    public List<NumberModel> getPaymentsCount(String partyId,
                                              List<String> shopIds,
                                              LocalDateTime from,
                                              LocalDateTime to) {
        String selectSql = "SELECT currency, count() as num from analytic.events_sink ";
        String groupedSql = " group by partyId, currency having partyId = ? ";

        List<Map<String, Object>> rows = splitQuery(partyId, shopIds, from, to, WHERE_TIME_PARAMS, groupedSql, selectSql);
        return countModelCommonRowsMapper.map(rows);
    }

    public List<SplitNumberModel> getPaymentsSplitAmount(String partyId,
                                                         List<String> shopIds,
                                                         LocalDateTime from,
                                                         LocalDateTime to,
                                                         SplitUnit splitUnit) {
        String groupBy = SplitUtils.initGroupByFunction(splitUnit);

        String selectSql = "SELECT " + groupBy + " , currency, sum(amount) as num from analytic.events_sink ";
        String groupedSql = " group by partyId, currency, " + groupBy + " having partyId = ? ";

        List<Map<String, Object>> rows = splitQuery(partyId, shopIds, from, to, WHERE_TIME_PARAMS, groupedSql, selectSql);
        return splitCostCommonRowsMapper.map(rows, splitUnit);
    }

    private List<Map<String, Object>> splitQuery(String partyId, List<String> shopIds, LocalDateTime from, LocalDateTime to, String whereSql, String groupedSql, String sql) {
        List<Object> params;

        long fromMillis = from.toInstant(ZoneOffset.UTC).toEpochMilli();
        long toMillis = to.toInstant(ZoneOffset.UTC).toEpochMilli();
        if (!CollectionUtils.isEmpty(shopIds)) {
            StringBuilder inList = QueryUtils.generateInList(shopIds);
            sql = sql + whereSql + " AND shopId " + inList + groupedSql;
            params = new ArrayList<>(Arrays.asList(from.toLocalDate(), to.toLocalDate(), fromMillis, toMillis, fromMillis, toMillis));
            params.addAll(shopIds);
            params.add(partyId);
        } else {
            sql = sql + whereSql + groupedSql;
            params = new ArrayList<>(Arrays.asList(from.toLocalDate(), to.toLocalDate(), fromMillis, toMillis, fromMillis, toMillis, partyId));
        }

        log.info("splitQuery sql: {} params: {}", sql, params);
        return clickHouseJdbcTemplate.queryForList(sql, params.toArray());
    }

    public List<SplitStatusNumberModel> getPaymentsSplitCount(String partyId,
                                                              List<String> shopIds,
                                                              LocalDateTime from,
                                                              LocalDateTime to,
                                                              SplitUnit splitUnit) {
        String groupBy = SplitUtils.initGroupByFunction(splitUnit);

        String selectSql = "SELECT " + groupBy + " , status, currency, count(concat(invoiceId, paymentId)) as num from analytic.events_sink ";
        String groupedSql = " group by partyId, currency, status, " + groupBy + " having partyId = ? ";

        List<Map<String, Object>> rows = splitQuery(partyId, shopIds, from, to, WHERE_TIME_PARAMS, groupedSql, selectSql);
        return splitStatusRowsMapper.map(rows, splitUnit);
    }

    public List<NamingDistribution> getPaymentsToolDistribution(String partyId, List<String> shopIds,
                                                                LocalDateTime from,
                                                                LocalDateTime to) {
        String sql = "SELECT %1$s, %3$s as naming_result," +
                "(SELECT count() from analytic.events_sink " +
                "where timestamp >= ? and timestamp <= ? AND eventTimeHour >= ? AND eventTimeHour <= ? AND eventTime >= ? AND eventTime <= ? AND %1$s %2$s " +
                "group by partyId) as total_count, count() * 100 / total_count as percent " +
                "from analytic.events_sink " +
                "where timestamp >= ? and timestamp <= ? AND eventTimeHour >= ? AND eventTimeHour <= ? AND eventTime >= ? AND eventTime <= ? AND %1$s %2$s " +
                "group by %1$s, %3$s";
        return queryNamingDistributions(sql, partyId, shopIds, from, to, PAYMENT_TOOL);
    }

    public List<NamingDistribution> getPaymentsErrorDistribution(String partyId, List<String> shopIds,
                                                                 LocalDateTime from,
                                                                 LocalDateTime to) {
        String sql = "SELECT %1$s, %3$s as naming_result," +
                "(SELECT count() from analytic.events_sink " +
                "where status='failed' and timestamp >= ? and timestamp <= ? AND eventTimeHour >= ? AND eventTimeHour <= ? AND eventTime >= ? AND eventTime <= ? AND %1$s %2$s " +
                "group by partyId) as total_count, count() * 100 / total_count as percent " +
                "from analytic.events_sink " +
                "where status='failed' and timestamp >= ? and timestamp <= ? AND eventTimeHour >= ? AND eventTimeHour <= ? AND eventTime >= ? AND eventTime <= ? AND %1$s %2$s " +
                "group by %1$s, %3$s ";
        return queryNamingDistributions(sql, partyId, shopIds, from, to, ERROR_REASON);
    }

    private List<NamingDistribution> queryNamingDistributions(String sql, String partyId, List<String> shopIds,
                                                              LocalDateTime from,
                                                              LocalDateTime to,
                                                              String name) {
        Object[] params = null;
        long fromMillis = from.toInstant(ZoneOffset.UTC).toEpochMilli();
        long toMillis = to.toInstant(ZoneOffset.UTC).toEpochMilli();
        LocalDate localDateFrom = from.toLocalDate();
        LocalDate localDateTo = to.toLocalDate();
        if (!CollectionUtils.isEmpty(shopIds)) {
            StringBuilder inList = QueryUtils.generateInList(shopIds);
            sql = String.format(sql, SHOP_ID, inList.toString(), name);
            List<Object> listParams = new ArrayList<>(Arrays.asList(localDateFrom, localDateTo, fromMillis, toMillis, fromMillis, toMillis));
            listParams.addAll(shopIds);
            params = doubleList(listParams).toArray();
        } else {
            sql = String.format(sql, PARTY_ID, " = ? ", name);
            params = new Object[]{localDateFrom, localDateTo, fromMillis, toMillis, fromMillis, toMillis, partyId, localDateFrom, localDateTo,
                    fromMillis, toMillis, fromMillis, toMillis, partyId};
        }

        List<Map<String, Object>> rows = clickHouseJdbcTemplate.queryForList(sql, params);
        return namingDistributionCommonRowsMapper.map(rows);
    }

    private ArrayList<Object> doubleList(List<Object> listParams) {
        ArrayList<Object> resultList = new ArrayList<>();
        resultList.addAll(listParams);
        resultList.addAll(listParams);
        return resultList;
    }

}
