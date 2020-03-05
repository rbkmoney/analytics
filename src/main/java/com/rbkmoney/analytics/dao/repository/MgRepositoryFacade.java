package com.rbkmoney.analytics.dao.repository;

import com.rbkmoney.analytics.dao.model.MgAdjustmentRow;
import com.rbkmoney.analytics.dao.model.MgPaymentSinkRow;
import com.rbkmoney.analytics.dao.model.MgRefundRow;
import com.rbkmoney.analytics.dao.repository.clickhouse.ClickHouseAdjustmentRepository;
import com.rbkmoney.analytics.dao.repository.clickhouse.ClickHousePaymentRepository;
import com.rbkmoney.analytics.dao.repository.clickhouse.ClickHouseRefundRepository;
import com.rbkmoney.analytics.dao.repository.postgres.PostgresBalanceChangesRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class MgRepositoryFacade {

    private final PostgresBalanceChangesRepository postgresBalanceChangesRepository;

    private final ClickHousePaymentRepository clickHousePaymentRepository;
    private final ClickHouseRefundRepository clickHouseRefundRepository;
    private final ClickHouseAdjustmentRepository clickHouseAdjustmentRepository;

    @Value("${repository.insert.enabled}")
    private boolean repositoryInsertEnabled;
    @Value("${repository.insert.logging.timeout:1000}")
    private int repositoryInsertLoggingTimeout;

    public void insertPayments(List<MgPaymentSinkRow> mgPaymentSinkRows) {
        if (repositoryInsertEnabled) {
            postgresBalanceChangesRepository.insertPayments(mgPaymentSinkRows);
            clickHousePaymentRepository.insertBatch(mgPaymentSinkRows);
        } else {
            log.info("mgPaymentSinkRows: {}", mgPaymentSinkRows);
            safeSleep();
        }
    }

    public void insertRefunds(List<MgRefundRow> mgRefundRows) {
        if (repositoryInsertEnabled) {
            postgresBalanceChangesRepository.insertRefunds(mgRefundRows);
            clickHouseRefundRepository.insertBatch(mgRefundRows);
        } else {
            log.info("mgRefundRows: {}", mgRefundRows);
            safeSleep();
        }
    }

    public void insertAdjustments(List<MgAdjustmentRow> mgAdjustmentRows) {
        if (repositoryInsertEnabled) {
            postgresBalanceChangesRepository.insertAdjustments(mgAdjustmentRows);
            clickHouseAdjustmentRepository.insertBatch(mgAdjustmentRows);
        } else {
            log.info("mgPaymentSinkRows: {}", mgAdjustmentRows);
            safeSleep();
        }
    }

    private void safeSleep() {
        try {
            Thread.sleep(repositoryInsertLoggingTimeout);
        } catch (InterruptedException e) {
            log.error("Exception when sleep e: ", e);
            Thread.currentThread().interrupt();
        }
    }
}
