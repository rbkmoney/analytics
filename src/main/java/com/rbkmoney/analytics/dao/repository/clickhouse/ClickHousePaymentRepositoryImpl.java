package com.rbkmoney.analytics.dao.repository.clickhouse;

import com.rbkmoney.analytics.dao.mapper.CommonRowsMapper;
import com.rbkmoney.analytics.dao.mapper.SplitRowsMapper;
import com.rbkmoney.analytics.dao.mapper.SplitStatusRowsMapper;
import com.rbkmoney.analytics.dao.model.*;
import com.rbkmoney.analytics.dao.repository.clickhouse.iface.ClickHousePaymentRepository;
import com.rbkmoney.analytics.dao.utils.QueryUtils;
import com.rbkmoney.analytics.dao.utils.SplitUtils;
import com.rbkmoney.analytics.utils.FileUtil;
import com.rbkmoney.analytics.utils.TimeParamUtils;
import com.rbkmoney.damsel.analytics.SplitUnit;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;
import ru.yandex.clickhouse.except.ClickHouseException;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class ClickHousePaymentRepositoryImpl implements ClickHousePaymentRepository {

    public static final String PAYMENT_TOOL = "paymentTool";
    public static final String ERROR_REASON = "errorReason";
    public static final String ERROR_CODE = "errorCode";
    public static final String WHERE_TIME_PARAMS = "where timestamp >= ? and timestamp <= ? AND eventTimeHour >= ? AND eventTimeHour <= ? AND eventTime >= ? AND eventTime <= ?";
    public static final String AND_STATUS_CAPTURED = " and status='captured'";
    public static final String ADDITIONAL_PARAMETERS = " %1s %2s";

    public static final String SELECT_BALANCES_SQL = FileUtil.getFile("scripts/select_current_balance.sql");
    public static final String SELECT_ERROR_DESCRIPTION = FileUtil.getFile("scripts/select_error_description.sql");
    public static final String SELECT_PAYMENT_TOOL_DESCRIPTION = FileUtil.getFile("scripts/select_payment_tool_description.sql");

    private final JdbcTemplate clickHouseJdbcTemplate;

    private final CommonRowsMapper<NumberModel> costCommonRowsMapper;
    private final CommonRowsMapper<NumberModel> countModelCommonRowsMapper;
    private final CommonRowsMapper<NamingDistribution> namingDistributionCommonRowsMapper;
    private final SplitRowsMapper splitCostCommonRowsMapper;
    private final SplitStatusRowsMapper splitStatusRowsMapper;

    @Override
    @Retryable(value = ClickHouseException.class, backoff = @Backoff(delay = 5000))
    public void insertBatch(List<PaymentRow> paymentRows) {
        if (paymentRows != null && !paymentRows.isEmpty()) {
            log.info("Batch start insert paymentRows: {} firstElement: {}", paymentRows.size(),
                    paymentRows.get(0).getInvoiceId());
            clickHouseJdbcTemplate.batchUpdate(ClickHousePaymentBatchPreparedStatementSetter.INSERT,
                    new ClickHousePaymentBatchPreparedStatementSetter(paymentRows));
            log.info("Batch inserted paymentRows: {} firstElement: {}", paymentRows.size(),
                    paymentRows.get(0).getInvoiceId());
        }
    }

    @Override
    public List<NumberModel> getAveragePayment(String partyId,
                                               List<String> shopIds,
                                               List<String> excludeShopIds,
                                               LocalDateTime from,
                                               LocalDateTime to) {
        String selectSql = "SELECT currency, avg(amount) as num from analytic.events_sink ";
        String groupedSql = " group by partyId, currency having partyId = ? ";

        List<Map<String, Object>> rows = splitQuery(partyId, shopIds, excludeShopIds, from, to,
                WHERE_TIME_PARAMS + AND_STATUS_CAPTURED + ADDITIONAL_PARAMETERS, groupedSql, selectSql);
        return costCommonRowsMapper.map(rows);
    }

    @Override
    public List<NumberModel> getPaymentsAmount(String partyId,
                                               List<String> shopIds,
                                               List<String> excludeShopIds,
                                               LocalDateTime from,
                                               LocalDateTime to) {
        String selectSql = "SELECT currency, sum(amount) as num from analytic.events_sink ";
        String groupedSql = " group by partyId, currency having partyId = ? ";

        List<Map<String, Object>> rows = splitQuery(partyId, shopIds, excludeShopIds, from, to,
                WHERE_TIME_PARAMS + AND_STATUS_CAPTURED + ADDITIONAL_PARAMETERS, groupedSql, selectSql);
        return costCommonRowsMapper.map(rows);
    }

    @Override
    public List<NumberModel> getPaymentsCount(String partyId,
                                              List<String> shopIds,
                                              List<String> excludeShopIds,
                                              LocalDateTime from,
                                              LocalDateTime to) {
        String selectSql = "SELECT currency, count() as num from analytic.events_sink ";
        String groupedSql = " group by partyId, currency having partyId = ? ";

        List<Map<String, Object>> rows = splitQuery(partyId, shopIds, excludeShopIds, from, to,
                WHERE_TIME_PARAMS + AND_STATUS_CAPTURED + ADDITIONAL_PARAMETERS, groupedSql, selectSql);
        return countModelCommonRowsMapper.map(rows);
    }

    @Override
    public List<SplitNumberModel> getPaymentsSplitAmount(String partyId,
                                                         List<String> shopIds,
                                                         List<String> excludeShopIds,
                                                         LocalDateTime from,
                                                         LocalDateTime to,
                                                         SplitUnit splitUnit) {
        String groupBy = SplitUtils.initGroupByFunction(splitUnit);

        String selectSql = "SELECT " + groupBy + " , currency, sum(amount) as num from analytic.events_sink ";
        String groupedSql = " group by partyId, currency, " + groupBy + " having partyId = ? ";

        List<Map<String, Object>> rows = splitQuery(partyId, shopIds, excludeShopIds, from, to,
                WHERE_TIME_PARAMS + AND_STATUS_CAPTURED + ADDITIONAL_PARAMETERS, groupedSql, selectSql);
        return splitCostCommonRowsMapper.map(rows, splitUnit);
    }

    private List<Map<String, Object>> splitQuery(String partyId,
                                                 List<String> shopIds,
                                                 List<String> excludeShopIds,
                                                 LocalDateTime from,
                                                 LocalDateTime to,
                                                 String whereSql,
                                                 String groupedSql,
                                                 String sql) {
        List<Object> params = TimeParamUtils.generateTimeParams(from, to);

        sql = String.format(sql + whereSql, QueryUtils.generateIdsSql(shopIds, params, QueryUtils::generateInList),
                QueryUtils.generateIdsSql(excludeShopIds, params, QueryUtils::generateNotInList)) + groupedSql;
        params.add(partyId);

        log.info("splitQuery sql: {} params: {}", sql, params);
        return clickHouseJdbcTemplate.queryForList(sql, params.toArray());
    }

    @Override
    public List<SplitStatusNumberModel> getPaymentsSplitCount(String partyId,
                                                              List<String> shopIds,
                                                              List<String> excludeShopIds,
                                                              LocalDateTime from,
                                                              LocalDateTime to,
                                                              SplitUnit splitUnit) {
        String groupBy = SplitUtils.initGroupByFunction(splitUnit);

        String selectSql = "SELECT " + groupBy + " , status, currency, count(concat(invoiceId, paymentId)) as num from analytic.events_sink ";
        String groupedSql = " group by partyId, currency, status, " + groupBy + " having partyId = ? ";

        List<Map<String, Object>> rows = splitQuery(partyId, shopIds, excludeShopIds, from, to, WHERE_TIME_PARAMS + ADDITIONAL_PARAMETERS, groupedSql, selectSql);
        return splitStatusRowsMapper.map(rows, splitUnit);
    }

    @Override
    public List<NamingDistribution> getPaymentsToolDistribution(String partyId,
                                                                List<String> shopIds,
                                                                List<String> excludeShopIds,
                                                                LocalDateTime from,
                                                                LocalDateTime to) {
        return queryNamingDistributions(SELECT_PAYMENT_TOOL_DESCRIPTION, partyId, shopIds, excludeShopIds, from, to, PAYMENT_TOOL);
    }

    @Override
    public List<NamingDistribution> getPaymentsErrorReasonDistribution(String partyId,
                                                                       List<String> shopIds,
                                                                       List<String> excludeShopIds,
                                                                       LocalDateTime from,
                                                                       LocalDateTime to) {
        return errorDistributionQuery(partyId, shopIds, excludeShopIds, from, to, ERROR_REASON);
    }

    @Override
    public List<NamingDistribution> getPaymentsErrorCodeDistribution(String partyId,
                                                                     List<String> shopIds,
                                                                     List<String> excludeShopIds,
                                                                     LocalDateTime from,
                                                                     LocalDateTime to) {
        return errorDistributionQuery(partyId, shopIds, excludeShopIds, from, to, ERROR_CODE);
    }

    private List<NamingDistribution> errorDistributionQuery(String partyId,
                                                            List<String> shopIds,
                                                            List<String> excludeShopIds,
                                                            LocalDateTime from,
                                                            LocalDateTime to,
                                                            String groupField) {
        return queryNamingDistributions(SELECT_ERROR_DESCRIPTION, partyId, shopIds, excludeShopIds, from, to, groupField);
    }

    private List<NamingDistribution> queryNamingDistributions(String sql,
                                                              String partyId,
                                                              List<String> shopIds,
                                                              List<String> excludeShopIds,
                                                              LocalDateTime from,
                                                              LocalDateTime to,
                                                              String name) {
        List<Object> timeParams = TimeParamUtils.generateTimeParams(from, to);
        timeParams.add(partyId);
        sql = String.format(sql, QueryUtils.generateIdsSql(shopIds, timeParams, QueryUtils::generateInList),
                QueryUtils.generateIdsSql(excludeShopIds, timeParams, QueryUtils::generateNotInList), name);
        Object[] params = doubleList(timeParams).toArray();

        log.info("queryNamingDistributions sql: {} params: {}", sql, params);
        List<Map<String, Object>> rows = clickHouseJdbcTemplate.queryForList(sql, params);
        return namingDistributionCommonRowsMapper.map(rows);
    }

    private ArrayList<Object> doubleList(List<Object> listParams) {
        ArrayList<Object> resultList = new ArrayList<>();
        resultList.addAll(listParams);
        resultList.addAll(listParams);
        return resultList;
    }

    @Override
    public List<NumberModel> getCurrentBalances(String partyId, List<String> shopIds, List<String> excludeShopIds) {
        LocalDate to = LocalDate.now();
        List<Object> params = new ArrayList<>();
        params.add(to);
        params.add(partyId);
        String sql = String.format(SELECT_BALANCES_SQL, QueryUtils.generateIdsSql(shopIds, params, QueryUtils::generateInList),
                QueryUtils.generateIdsSql(excludeShopIds, params, QueryUtils::generateNotInList));
        params = Collections.nCopies(4, params).stream()
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
        log.info("getCurrentBalances sql: {} params: {}", sql, params);
        List<Map<String, Object>> rows = clickHouseJdbcTemplate.queryForList(sql, params.toArray());
        return costCommonRowsMapper.map(rows);
    }

}
