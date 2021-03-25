package com.rbkmoney.analytics.dao.repository.postgres;

import com.rbkmoney.analytics.dao.model.*;
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
    public void insertPayments(List<PaymentRow> paymentRows) {
        if (CollectionUtils.isEmpty(paymentRows)) {
            return;
        }

        log.info("Batch start insert paymentRows: {} firstElement: {}",
                paymentRows.size(),
                paymentRows.get(0).getInvoiceId());

        postgresJdbcTemplate.batchUpdate(
                INSERT,
                new PostgresPaymentBatchPreparedStatementSetter(paymentRows));

        log.info("Batch inserted paymentRows: {} firstElement: {}",
                paymentRows.size(),
                paymentRows.get(0).getInvoiceId());
    }

    @Retryable(value = SQLException.class, backoff = @Backoff(delay = 5000))
    public void insertAdjustments(List<AdjustmentRow> adjustmentRows) {
        if (CollectionUtils.isEmpty(adjustmentRows)) {
            return;
        }

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
    public void insertChargebacks(List<ChargebackRow> chargebackRows) {
        if (CollectionUtils.isEmpty(chargebackRows)) {
            return;
        }

        log.info("Batch start insert chargebackRows: {} firstElement: {}",
                chargebackRows.size(),
                chargebackRows.get(0).getInvoiceId());

        postgresJdbcTemplate.batchUpdate(
                INSERT,
                new PostgresChargebackBatchPreparedStatementSetter(chargebackRows));

        log.info("Batch inserted chargebackRows: {} firstElement: {}",
                chargebackRows.size(),
                chargebackRows.get(0).getInvoiceId());
    }

    @Retryable(value = SQLException.class, backoff = @Backoff(delay = 5000))
    public void insertRefunds(List<RefundRow> refundRows) {
        if (CollectionUtils.isEmpty(refundRows)) {
            return;
        }

        log.info("Batch start insert refundRows: {} firstElement: {}",
                refundRows.size(),
                refundRows.get(0).getInvoiceId());

        postgresJdbcTemplate.batchUpdate(
                INSERT,
                new PostgresRefundBatchPreparedStatementSetter(refundRows));

        log.info("Batch inserted refundRows: {} firstElement: {}",
                refundRows.size(),
                refundRows.get(0).getInvoiceId());
    }

    @Retryable(value = SQLException.class, backoff = @Backoff(delay = 5000))
    public void insertPayouts(List<PayoutRow> payoutRows) {
        if (CollectionUtils.isEmpty(payoutRows)) {
            return;
        }

        log.info("Batch start insert payoutRows: {} firstElement: {}",
                payoutRows.size(),
                payoutRows.get(0).getPayoutId());

        postgresJdbcTemplate.batchUpdate(
                INSERT,
                new PostgresPayoutBatchPreparedStatementSetter(payoutRows));

        log.info("Batch inserted payoutRows: {} firstElement: {}",
                payoutRows.size(),
                payoutRows.get(0).getPayoutId());
    }
}
