package com.rbkmoney.analytics.dao.repository;

import com.rbkmoney.analytics.dao.mapper.CommonRowsMapper;
import com.rbkmoney.analytics.dao.mapper.SplitRowsMapper;
import com.rbkmoney.analytics.dao.mapper.SplitStatusRowsMapper;
import com.rbkmoney.analytics.dao.model.*;
import com.rbkmoney.analytics.dao.utils.DateFilterUtils;
import com.rbkmoney.analytics.dao.utils.QueryUtils;
import com.rbkmoney.analytics.dao.utils.SplitUtils;
import com.rbkmoney.damsel.analytics.SplitUnit;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.sql.Date;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class MgPaymentRepository {

    public static final String SHOP_ID = "shopId";
    public static final String PARTY_ID = "partyId";
    public static final String PAYMENT_TOOL = "paymentTool";
    public static final String ERROR_REASON = "errorReason";

    private final JdbcTemplate jdbcTemplate;

    private final CommonRowsMapper<NumberModel> costCommonRowsMapper;
    private final CommonRowsMapper<NumberModel> countModelCommonRowsMapper;
    private final CommonRowsMapper<NamingDistribution> namingDistributionCommonRowsMapper;
    private final SplitRowsMapper splitCostCommonRowsMapper;
    private final SplitStatusRowsMapper splitStatusRowsMapper;

    public void insertBatch(List<MgPaymentSinkRow> mgPaymentSinkRows) {
        if (mgPaymentSinkRows != null && !mgPaymentSinkRows.isEmpty()) {
            jdbcTemplate.batchUpdate(MgPaymentBatchPreparedStatementSetter.INSERT,
                    new MgPaymentBatchPreparedStatementSetter(mgPaymentSinkRows));
            log.info("Batch inserted mgPaymentSinkRows: {} firstElement: {}", mgPaymentSinkRows.size(),
                    mgPaymentSinkRows.get(0).getInvoiceId());
        }
    }

    public List<NumberModel> getAveragePayment(String partyId,
                                               List<String> shopIds,
                                               Long from,
                                               Long to) {
        Date dateFrom = DateFilterUtils.parseDate(from);
        Date dateTo = DateFilterUtils.parseDate(to);

        String selectSql = "SELECT currency, sum(amount * sign) / sum(sign) as num " +
                "from analytic.events_sink ";
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

        List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql, params.toArray());
        return costCommonRowsMapper.map(rows);
    }

    public List<NumberModel> getPaymentsAmount(String partyId,
                                               List<String> shopIds,
                                               Long from,
                                               Long to) {
        Date dateFrom = DateFilterUtils.parseDate(from);
        Date dateTo = DateFilterUtils.parseDate(to);

        String selectSql = "SELECT currency, sum(amount * sign) as num " +
                "from analytic.events_sink ";
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

        List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql, params.toArray());
        return costCommonRowsMapper.map(rows);
    }

    public List<NumberModel> getPaymentsCount(String partyId,
                                              List<String> shopIds,
                                              Long from,
                                              Long to) {
        Date dateFrom = DateFilterUtils.parseDate(from);
        Date dateTo = DateFilterUtils.parseDate(to);

        String selectSql = "SELECT currency, sum(sign) as num " +
                "from analytic.events_sink ";
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

        List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql, params.toArray());
        return countModelCommonRowsMapper.map(rows);
    }

    public List<SplitNumberModel> getPaymentsSplitAmount(String partyId,
                                                         List<String> shopIds,
                                                         Long from,
                                                         Long to,
                                                         SplitUnit splitUnit) {
        String groupBy = SplitUtils.initGroupByFunction(splitUnit);

        String selectSql = "SELECT " + groupBy + " , currency, sum(amount * sign) as num " +
                "from analytic.events_sink ";
        String whereSql = "where timestamp >= ? and timestamp <= ? AND eventTimeHour >= ? AND eventTimeHour <= ? AND eventTime >= ? AND eventTime <= ?";
        String groupedSql = " group by partyId, currency, " + groupBy +
                " having partyId = ? " +
                " AND sum(sign) > 0";

        String sql = selectSql;
        List<Object> params = null;

        Date dateFrom = DateFilterUtils.parseDate(from);
        Date dateTo = DateFilterUtils.parseDate(to);
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

        List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql, params.toArray());
        return splitCostCommonRowsMapper.map(rows, splitUnit);
    }

    public List<SplitStatusNumberModel> getPaymentsSplitCount(String partyId,
                                                  List<String> shopIds,
                                                  Long from,
                                                  Long to,
                                                  SplitUnit splitUnit) {
        String groupBy = SplitUtils.initGroupByFunction(splitUnit);

        String selectSql = "SELECT " + groupBy + " , status, currency, count(sign) as num " +
                "from analytic.events_sink ";
        String whereSql = "where timestamp >= ? and timestamp <= ? AND eventTimeHour >= ? AND eventTimeHour <= ? AND eventTime >= ? AND eventTime <= ?";
        String groupedSql = " group by partyId, currency, status, " + groupBy +
                " having partyId = ? " +
                " AND sum(sign) > 0";

        String sql = selectSql;
        List<Object> params = null;

        Date dateFrom = DateFilterUtils.parseDate(from);
        Date dateTo = DateFilterUtils.parseDate(to);
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

        List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql, params.toArray());
        return splitStatusRowsMapper.map(rows, splitUnit);
    }

    public List<NamingDistribution> getPaymentsToolDistribution(String partyId, List<String> shopIds, Long from, Long to) {
        String sql = "SELECT %1$s, %3$s as naming_result," +
                "(SELECT sum(sign) from analytic.events_sink " +
                "where timestamp >= ? and timestamp <= ? AND eventTimeHour >= ? AND eventTimeHour <= ? AND eventTime >= ? AND eventTime <= ? AND %1$s %2$s " +
                "group by partyId " +
                "having sum(sign) > 0) as total_count, sum(sign) * 100 / total_count as percent " +
                "from analytic.events_sink " +
                "where timestamp >= ? and timestamp <= ? AND eventTimeHour >= ? AND eventTimeHour <= ? AND eventTime >= ? AND eventTime <= ? AND %1$s %2$s " +
                "group by %1$s, %3$s " +
                "having sum(sign) > 0";
        return queryNamingDistributions(sql, partyId, shopIds, from, to, PAYMENT_TOOL);
    }

    public List<NamingDistribution> getPaymentsErrorDistribution(String partyId, List<String> shopIds, Long from, Long to) {
        String sql = "SELECT %1$s, %3$s as naming_result," +
                "(SELECT sum(sign) from analytic.events_sink " +
                "where status='failed' and timestamp >= ? and timestamp <= ? AND eventTimeHour >= ? AND eventTimeHour <= ? AND eventTime >= ? AND eventTime <= ? AND %1$s %2$s " +
                "group by partyId " +
                "having sum(sign) > 0) as total_count, sum(sign) * 100 / total_count as percent " +
                "from analytic.events_sink " +
                "where status='failed' and timestamp >= ? and timestamp <= ? AND eventTimeHour >= ? AND eventTimeHour <= ? AND eventTime >= ? AND eventTime <= ? AND %1$s %2$s " +
                "group by %1$s, %3$s " +
                "having sum(sign) > 0";
        return queryNamingDistributions(sql, partyId, shopIds, from, to, ERROR_REASON);
    }

    private List<NamingDistribution> queryNamingDistributions(String sql, String partyId, List<String> shopIds, Long from, Long to, String name) {
        Object[] params = null;
        Date dateFrom = DateFilterUtils.parseDate(from);
        Date dateTo = DateFilterUtils.parseDate(to);

        if (!CollectionUtils.isEmpty(shopIds)) {
            StringBuilder inList = QueryUtils.generateInList(shopIds);
            sql = String.format(sql, SHOP_ID, inList.toString(), name);
            List<Object> listParams = new ArrayList<>(Arrays.asList(dateFrom, dateTo, from, to, from, to));
            listParams.addAll(shopIds);
            params = doubleList(listParams).toArray();
        } else {
            sql = String.format(sql, PARTY_ID, " = ? ", name);
            params = new Object[]{dateFrom, dateTo, from, to, from, to, partyId, dateFrom, dateTo, from, to, from, to, partyId};
        }

        List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql, params);
        return namingDistributionCommonRowsMapper.map(rows);
    }

    private ArrayList<Object> doubleList(List<Object> listParams) {
        ArrayList<Object> resultList = new ArrayList<>();
        resultList.addAll(listParams);
        resultList.addAll(listParams);
        return resultList;
    }

}
