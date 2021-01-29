package com.rbkmoney.analytics.dao.repository;

import com.rbkmoney.analytics.constant.*;
import com.rbkmoney.analytics.dao.model.*;
import com.rbkmoney.analytics.dao.repository.clickhouse.*;
import com.rbkmoney.analytics.dao.repository.clickhouse.iface.ClickHousePaymentRepository;
import com.rbkmoney.analytics.dao.repository.postgres.PostgresBalanceChangesRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;

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
    private final ClickHousePayoutRepository clickHousePayoutRepository;

    public void insertPayments(List<PaymentRow> paymentRows) {
        List<PaymentRow> capturedPayments = paymentRows.stream()
                .filter(paymentRow -> paymentRow.getStatus() == PaymentStatus.captured)
                .collect(toList());

        postgresBalanceChangesRepository.insertPayments(capturedPayments);
        log.info("RepositoryFacade PG inserted insertPayments: {}", capturedPayments.size());
        clickHousePaymentRepository.insertBatch(paymentRows);
        log.info("RepositoryFacade CH inserted insertPayments: {}", paymentRows.size());
    }

    public void insertRefunds(List<RefundRow> refundRows) {
        List<RefundRow> succeededRefunds = refundRows.stream()
                .filter(refundRow -> refundRow.getStatus() == RefundStatus.succeeded)
                .collect(toList());

        postgresBalanceChangesRepository.insertRefunds(succeededRefunds);
        log.info("RepositoryFacade PG inserted insertRefunds: {}", succeededRefunds.size());
        clickHouseRefundRepository.insertBatch(refundRows);
        log.info("RepositoryFacade CH inserted insertRefunds: {}", refundRows.size());
    }

    public void insertAdjustments(List<AdjustmentRow> adjustmentRows) {
        List<AdjustmentRow> capturedAdjustments = adjustmentRows.stream()
                .filter(adjustmentRow -> adjustmentRow.getStatus() == AdjustmentStatus.captured)
                .collect(toList());

        postgresBalanceChangesRepository.insertAdjustments(capturedAdjustments);
        log.info("RepositoryFacade PG inserted insertAdjustments: {}", capturedAdjustments.size());
        clickHouseAdjustmentRepository.insertBatch(adjustmentRows);
        log.info("RepositoryFacade CH inserted insertAdjustments: {}", adjustmentRows.size());
    }

    public void insertChargebacks(List<ChargebackRow> chargebackRows) {
        List<ChargebackRow> acceptedChargebacks = chargebackRows.stream()
                .filter(chargebackRow -> chargebackRow.getStatus() == ChargebackStatus.accepted)
                .collect(toList());

        postgresBalanceChangesRepository.insertChargebacks(acceptedChargebacks);
        log.info("RepositoryFacade PG inserted insertChargebacks: {}", acceptedChargebacks.size());
        clickHouseChargebackRepository.insertBatch(chargebackRows);
        log.info("RepositoryFacade CH inserted insertChargebacks: {}", chargebackRows.size());
    }

    public void insertPayouts(List<PayoutRow> payoutRows) {
        List<PayoutRow> paidPayouts = payoutRows.stream()
                .filter(payoutRow -> payoutRow.getStatus() == PayoutStatus.paid
                        || (payoutRow.getStatus() == PayoutStatus.cancelled && payoutRow.isCancelledAfterBeingPaid()))
                .collect(toList());

        postgresBalanceChangesRepository.insertPayouts(paidPayouts);
        log.info("RepositoryFacade PG inserted insertPayouts: {}", paidPayouts.size());
        clickHousePayoutRepository.insertBatch(payoutRows);
        log.info("RepositoryFacade CH inserted insertPayouts: {}", payoutRows.size());
    }

}
