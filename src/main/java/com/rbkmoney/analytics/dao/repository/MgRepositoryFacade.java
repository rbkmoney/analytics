package com.rbkmoney.analytics.dao.repository;

import com.rbkmoney.analytics.constant.AdjustmentStatus;
import com.rbkmoney.analytics.constant.ChargebackStatus;
import com.rbkmoney.analytics.constant.PaymentStatus;
import com.rbkmoney.analytics.constant.RefundStatus;
import com.rbkmoney.analytics.dao.model.MgAdjustmentRow;
import com.rbkmoney.analytics.dao.model.MgChargebackRow;
import com.rbkmoney.analytics.dao.model.MgPaymentSinkRow;
import com.rbkmoney.analytics.dao.model.MgRefundRow;
import com.rbkmoney.analytics.dao.repository.clickhouse.ClickHouseAdjustmentRepository;
import com.rbkmoney.analytics.dao.repository.clickhouse.ClickHouseChargebackRepository;
import com.rbkmoney.analytics.dao.repository.clickhouse.ClickHousePaymentRepository;
import com.rbkmoney.analytics.dao.repository.clickhouse.ClickHouseRefundRepository;
import com.rbkmoney.analytics.dao.repository.postgres.PostgresBalanceChangesRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class MgRepositoryFacade {

    private final PostgresBalanceChangesRepository postgresBalanceChangesRepository;

    private final ClickHousePaymentRepository clickHousePaymentRepository;
    private final ClickHouseRefundRepository clickHouseRefundRepository;
    private final ClickHouseAdjustmentRepository clickHouseAdjustmentRepository;
    private final ClickHouseChargebackRepository clickHouseChargebackRepository;

    @Value("${repository.insert.enabled}")
    private boolean repositoryInsertEnabled;
    @Value("${repository.insert.logging.timeout:1000}")
    private int repositoryInsertLoggingTimeout;

    public void insertPayments(List<MgPaymentSinkRow> mgPaymentSinkRows) {
        List<MgPaymentSinkRow> filteredRow = filterRows(mgPaymentSinkRows,
                mgPaymentSinkRow -> mgPaymentSinkRow.getStatus() == PaymentStatus.captured);
        postgresBalanceChangesRepository.insertPayments(filteredRow);
        log.info("MgRepositoryFacade PG inserted insertPayments: {}", filteredRow.size());
        clickHousePaymentRepository.insertBatch(mgPaymentSinkRows);
        log.info("MgRepositoryFacade CH inserted insertPayments: {}", mgPaymentSinkRows.size());
    }

    public void insertRefunds(List<MgRefundRow> mgRefundRows) {
        List<MgRefundRow> filteredRow = filterRows(mgRefundRows,
                mgRefundRow -> mgRefundRow.getStatus() == RefundStatus.succeeded);
        postgresBalanceChangesRepository.insertRefunds(filteredRow);
        log.info("MgRepositoryFacade PG inserted insertRefunds: {}", filteredRow.size());
        clickHouseRefundRepository.insertBatch(mgRefundRows);
        log.info("MgRepositoryFacade CH inserted insertRefunds: {}", mgRefundRows.size());
    }

    public void insertAdjustments(List<MgAdjustmentRow> mgAdjustmentRows) {
        List<MgAdjustmentRow> filteredRow = filterRows(mgAdjustmentRows,
                mgRefundRow -> mgRefundRow.getStatus() == AdjustmentStatus.captured);
        postgresBalanceChangesRepository.insertAdjustments(filteredRow);
        log.info("MgRepositoryFacade PG inserted insertAdjustments: {}", filteredRow.size());
        clickHouseAdjustmentRepository.insertBatch(mgAdjustmentRows);
        log.info("MgRepositoryFacade CH inserted insertAdjustments: {}", mgAdjustmentRows.size());
    }

    public void insertChargebacks(List<MgChargebackRow> mgChargebackRows) {
        List<MgChargebackRow> filteredRow = filterRows(mgChargebackRows,
                mgRefundRow -> mgRefundRow.getStatus() == ChargebackStatus.accepted);
        postgresBalanceChangesRepository.insertChargebacks(filteredRow);
        log.info("MgRepositoryFacade PG inserted insertChargebacks: {}", filteredRow.size());
        clickHouseChargebackRepository.insertBatch(mgChargebackRows);
        log.info("MgRepositoryFacade CH inserted insertChargebacks: {}", mgChargebackRows.size());
    }

    private <T> List<T> filterRows(List<T> rows, Predicate<T> predicate) {
        return rows.stream()
                .filter(predicate::test)
                .collect(Collectors.toList());
    }

}
