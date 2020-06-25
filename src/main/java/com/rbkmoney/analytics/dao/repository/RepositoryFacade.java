package com.rbkmoney.analytics.dao.repository;

import com.rbkmoney.analytics.constant.AdjustmentStatus;
import com.rbkmoney.analytics.constant.ChargebackStatus;
import com.rbkmoney.analytics.constant.PaymentStatus;
import com.rbkmoney.analytics.constant.RefundStatus;
import com.rbkmoney.analytics.dao.model.*;
import com.rbkmoney.analytics.dao.repository.clickhouse.ClickHouseAdjustmentRepository;
import com.rbkmoney.analytics.dao.repository.clickhouse.ClickHouseChargebackRepository;
import com.rbkmoney.analytics.dao.repository.clickhouse.ClickHousePaymentRepository;
import com.rbkmoney.analytics.dao.repository.clickhouse.ClickHouseRefundRepository;
import com.rbkmoney.analytics.dao.repository.postgres.PostgresBalanceChangesRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.function.Predicate;

import static java.util.stream.Collectors.toList;

@Slf4j
@Service
@RequiredArgsConstructor
public class RepositoryFacade {

    private final PostgresBalanceChangesRepository postgresBalanceChangesRepository;

    private final ClickHousePaymentRepository clickHousePaymentRepository;
    private final ClickHouseRefundRepository clickHouseRefundRepository;
    private final ClickHouseAdjustmentRepository clickHouseAdjustmentRepository;
    private final ClickHouseChargebackRepository clickHouseChargebackRepository;

    public void insertPayments(List<PaymentRow> paymentRows) {
        List<PaymentRow> filteredRow = filterRows(paymentRows,
                paymentRow -> paymentRow.getStatus() == PaymentStatus.captured);
        postgresBalanceChangesRepository.insertPayments(filteredRow);
        log.info("RepositoryFacade PG inserted insertPayments: {}", filteredRow.size());
        clickHousePaymentRepository.insertBatch(paymentRows);
        log.info("RepositoryFacade CH inserted insertPayments: {}", paymentRows.size());
    }

    public void insertRefunds(List<RefundRow> refundRows) {
        List<RefundRow> filteredRow = filterRows(refundRows,
                refundRow -> refundRow.getStatus() == RefundStatus.succeeded);
        postgresBalanceChangesRepository.insertRefunds(filteredRow);
        log.info("RepositoryFacade PG inserted insertRefunds: {}", filteredRow.size());
        clickHouseRefundRepository.insertBatch(refundRows);
        log.info("RepositoryFacade CH inserted insertRefunds: {}", refundRows.size());
    }

    public void insertAdjustments(List<AdjustmentRow> adjustmentRows) {
        List<AdjustmentRow> filteredRow = filterRows(adjustmentRows,
                adjustmentRow -> adjustmentRow.getStatus() == AdjustmentStatus.captured);
        postgresBalanceChangesRepository.insertAdjustments(filteredRow);
        log.info("RepositoryFacade PG inserted insertAdjustments: {}", filteredRow.size());
        clickHouseAdjustmentRepository.insertBatch(adjustmentRows);
        log.info("RepositoryFacade CH inserted insertAdjustments: {}", adjustmentRows.size());
    }

    public void insertChargebacks(List<ChargebackRow> chargebackRows) {
        List<ChargebackRow> filteredRow = filterRows(chargebackRows,
                chargebackRow -> chargebackRow.getStatus() == ChargebackStatus.accepted);
        postgresBalanceChangesRepository.insertChargebacks(filteredRow);
        log.info("RepositoryFacade PG inserted insertChargebacks: {}", filteredRow.size());
        clickHouseChargebackRepository.insertBatch(chargebackRows);
        log.info("RepositoryFacade CH inserted insertChargebacks: {}", chargebackRows.size());
    }

    public void insertPayouts(List<PayoutRow> payoutRow) {
        // TODO [a.romanov]: impl
        throw new UnsupportedOperationException();
    }

    private <T> List<T> filterRows(List<T> rows, Predicate<T> predicate) {
        return rows.stream()
                .filter(predicate)
                .collect(toList());
    }

}
