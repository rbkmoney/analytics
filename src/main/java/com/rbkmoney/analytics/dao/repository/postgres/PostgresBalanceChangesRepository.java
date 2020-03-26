package com.rbkmoney.analytics.dao.repository.postgres;

import com.rbkmoney.analytics.dao.model.MgAdjustmentRow;
import com.rbkmoney.analytics.dao.model.MgPaymentSinkRow;
import com.rbkmoney.analytics.dao.model.MgRefundRow;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.EnableRetry;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
import java.util.List;

@Slf4j
@Service
@EnableRetry
@RequiredArgsConstructor
public class PostgresBalanceChangesRepository {

    private static final String INSERT = "INSERT INTO analytics.balance_change " +
            "(id, event_created_at, party_id, shop_id, amount, currency) " +
            "VALUES (?, ?, ?, ?, ?, ?) " +
            "ON CONFLICT (id) DO NOTHING";

    private final JdbcTemplate postgresJdbcTemplate;

    @Retryable(value = SQLException.class, backoff = @Backoff(delay = 5000))
    public void insertPayments(List<MgPaymentSinkRow> mgPaymentSinkRows) {
        if (CollectionUtils.isEmpty(mgPaymentSinkRows)) return;

        log.info("Batch start insert mgPaymentSinkRows: {} firstElement: {}",
                mgPaymentSinkRows.size(),
                mgPaymentSinkRows.get(0).getInvoiceId());

        postgresJdbcTemplate.batchUpdate(
                INSERT,
                new PostgresPaymentBatchPreparedStatementSetter(mgPaymentSinkRows));

        log.info("Batch inserted mgPaymentSinkRows: {} firstElement: {}",
                mgPaymentSinkRows.size(),
                mgPaymentSinkRows.get(0).getInvoiceId());
    }

    @Retryable(value = SQLException.class, backoff = @Backoff(delay = 5000))
    public void insertAdjustments(List<MgAdjustmentRow> adjustmentRows) {
        if (CollectionUtils.isEmpty(adjustmentRows)) return;

        log.info("Batch start insert adjustmentRows: {} firstElement: {}",
                adjustmentRows.size(),
                adjustmentRows.get(0).getInvoiceId());

        postgresJdbcTemplate.batchUpdate(
                INSERT,
                new PostgresAdjustmentBatchPreparedStatementSetter(adjustmentRows));

        log.info("Batch inserted adjustmentRows: {} firstElement: {}",
                adjustmentRows.size(),
                adjustmentRows.get(0).getInvoiceId());
    }

    @Retryable(value = SQLException.class, backoff = @Backoff(delay = 5000))
    public void insertRefunds(List<MgRefundRow> mgRefundRows) {
        if (CollectionUtils.isEmpty(mgRefundRows)) return;

        log.info("Batch start insert mgRefundRows: {} firstElement: {}",
                mgRefundRows.size(),
                mgRefundRows.get(0).getInvoiceId());

        postgresJdbcTemplate.batchUpdate(
                INSERT,
                new PostgresRefundBatchPreparedStatementSetter(mgRefundRows));

        log.info("Batch inserted mgRefundRows: {} firstElement: {}",
                mgRefundRows.size(),
                mgRefundRows.get(0).getInvoiceId());
    }
}