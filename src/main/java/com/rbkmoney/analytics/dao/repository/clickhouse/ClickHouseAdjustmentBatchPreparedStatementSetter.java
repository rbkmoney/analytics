package com.rbkmoney.analytics.dao.repository.clickhouse;

import com.rbkmoney.analytics.dao.model.MgAdjustmentRow;
import com.rbkmoney.analytics.domain.CashFlowResult;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;

@RequiredArgsConstructor
public class ClickHouseAdjustmentBatchPreparedStatementSetter implements BatchPreparedStatementSetter {

    public static final String INSERT = "INSERT INTO analytic.events_sink_adjustment " +
            "(timestamp, eventTime, eventTimeHour, partyId, shopId, email, " +
            "amount, guaranteeDeposit, systemFee, providerFee, externalFee, " +
            "oldAmount, oldGuaranteeDeposit, oldSystemFee, oldProviderFee, oldExternalFee, " +
            "currency, providerName, status, errorCode, errorReason,  invoiceId, paymentId, adjustmentId, sequenceId, ip, " +
            "fingerprint, cardToken, paymentSystem, digitalWalletProvider, digitalWalletToken, cryptoCurrency, mobileOperator," +
            "paymentCountry, bankCountry)" +
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private final List<MgAdjustmentRow> batch;

    @Override
    public void setValues(PreparedStatement ps, int i) throws SQLException {
        MgAdjustmentRow row = batch.get(i);
        int l = 1;
        ps.setObject(l++, row.getEventTime().toLocalDate());
        ps.setLong(l++, row.getEventTime().toEpochSecond(ZoneOffset.UTC));
        ps.setLong(l++, row.getEventTime().toInstant(ZoneOffset.UTC).truncatedTo(ChronoUnit.HOURS).toEpochMilli());

        ps.setString(l++, row.getPartyId());
        ps.setString(l++, row.getShopId());

        ps.setString(l++, row.getEmail());

        CashFlowResult cashFlowResult = row.getCashFlowResult();
        ps.setLong(l++, cashFlowResult.getAmount());
        ps.setLong(l++, cashFlowResult.getGuaranteeDeposit());
        ps.setLong(l++, cashFlowResult.getSystemFee());
        ps.setLong(l++, cashFlowResult.getExternalFee());
        ps.setLong(l++, cashFlowResult.getProviderFee());

        CashFlowResult oldCashFlowResult = row.getOldCashFlowResult();
        ps.setLong(l++, oldCashFlowResult.getAmount());
        ps.setLong(l++, oldCashFlowResult.getGuaranteeDeposit());
        ps.setLong(l++, oldCashFlowResult.getSystemFee());
        ps.setLong(l++, oldCashFlowResult.getExternalFee());
        ps.setLong(l++, oldCashFlowResult.getProviderFee());

        ps.setString(l++, row.getCurrency());

        ps.setString(l++, row.getProvider());

        ps.setString(l++, row.getStatus().name());

        ps.setString(l++, row.getErrorCode());
        ps.setString(l++, row.getErrorReason());

        ps.setString(l++, row.getInvoiceId());
        ps.setString(l++, row.getPaymentId());
        ps.setString(l++, row.getAdjustmentId());
        ps.setLong(l++, row.getSequenceId());

        ps.setString(l++, row.getIp());

        ps.setString(l++, row.getFingerprint());
        ps.setString(l++, row.getCardToken());
        ps.setString(l++, row.getPaymentSystem());
        ps.setString(l++, row.getDigitalWalletProvider());
        ps.setString(l++, row.getDigitalWalletToken());
        ps.setString(l++, row.getCryptoCurrency());
        ps.setString(l++, row.getMobileOperator());

        ps.setString(l++, row.getPaymentCountry());
        ps.setString(l, row.getBankCountry());
    }

    @Override
    public int getBatchSize() {
        return batch.size();
    }
}
