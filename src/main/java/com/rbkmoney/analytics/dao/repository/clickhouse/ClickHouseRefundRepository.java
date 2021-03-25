package com.rbkmoney.analytics.dao.repository.clickhouse;

import com.rbkmoney.analytics.dao.mapper.CommonRowsMapper;
import com.rbkmoney.analytics.dao.model.NumberModel;
import com.rbkmoney.analytics.dao.model.RefundRow;
import com.rbkmoney.analytics.dao.utils.QueryUtils;
import com.rbkmoney.analytics.utils.FileUtil;
import com.rbkmoney.analytics.utils.TimeParamUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;
import ru.yandex.clickhouse.except.ClickHouseException;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class ClickHouseRefundRepository {

    public static final String SELECT_REFUND_PAYMENT_AMOUNT =
            FileUtil.getFile("scripts/select_refund_payment_amount.sql");
    private final JdbcTemplate clickHouseJdbcTemplate;
    private final CommonRowsMapper<NumberModel> costCommonRowsMapper;

    @Retryable(value = ClickHouseException.class, backoff = @Backoff(delay = 5000))
    public void insertBatch(List<RefundRow> refundRows) {
        if (refundRows != null && !refundRows.isEmpty()) {
            clickHouseJdbcTemplate.batchUpdate(ClickHouseRefundBatchPreparedStatementSetter.INSERT,
                    new ClickHouseRefundBatchPreparedStatementSetter(refundRows)
            );
            log.info("Batch inserted refundRows: {} firstElement: {}", refundRows.size(),
                    refundRows.get(0).getInvoiceId());
        }
    }

    public List<NumberModel> getPaymentsAmount(String partyId,
                                               List<String> shopIds,
                                               List<String> excludeShopIds,
                                               LocalDateTime from,
                                               LocalDateTime to) {
        List<Object> params = TimeParamUtils.generateTimeParams(from, to);
        String sql = String.format(SELECT_REFUND_PAYMENT_AMOUNT,
                QueryUtils.generateIdsSql(shopIds, params, QueryUtils::generateInList),
                QueryUtils.generateIdsSql(excludeShopIds, params, QueryUtils::generateNotInList));
        params.add(partyId);

        log.info("ClickHouseRefundRepository getPaymentsAmount sql: {} params: {}", sql, params);
        List<Map<String, Object>> rows = clickHouseJdbcTemplate.queryForList(sql, params.toArray());
        return costCommonRowsMapper.map(rows);
    }

}
