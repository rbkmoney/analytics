package com.rbkmoney.analytics.dao.repository.clickhouse;

import com.rbkmoney.analytics.constant.ClickHouseUtilsValue;
import com.rbkmoney.analytics.dao.model.RefundRow;
import com.rbkmoney.analytics.domain.CashFlowResult;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;

@RequiredArgsConstructor
public class ClickHouseRefundBatchPreparedStatementSetter implements BatchPreparedStatementSetter {

    public static final String INSERT = "INSERT INTO analytic.events_sink_refund " +
            "(timestamp, eventTime, eventTimeHour, partyId, shopId, email, " +
            "amount, guaranteeDeposit, systemFee, providerFee, externalFee, currency, providerName, " +
            "status, errorCode, errorReason,  invoiceId, paymentId, refundId, sequenceId, ip, " +
            "fingerprint,cardToken, paymentSystem, digitalWalletProvider, digitalWalletToken, cryptoCurrency, mobileOperator," +
            "paymentCountry, bankCountry, paymentTime, providerId, terminal)" +
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private final List<RefundRow> batch;

    @Override
    public void setValues(PreparedStatement ps, int i) throws SQLException {
        RefundRow row = batch.get(i);
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
        ps.setLong(l++, cashFlowResult.getProviderFee());
        ps.setLong(l++, cashFlowResult.getExternalFee());

        ps.setString(l++, row.getCurrency());

        ps.setString(l++, row.getProvider());

        ps.setString(l++, row.getStatus().name());

        ps.setString(l++, row.getErrorCode());
        ps.setString(l++, row.getErrorReason());

        ps.setString(l++, row.getInvoiceId());
        ps.setString(l++, row.getPaymentId());
        ps.setString(l++, row.getRefundId());
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
        ps.setString(l++, row.getBankCountry());

        ps.setLong(l++, row.getPaymentTime().toEpochSecond(ZoneOffset.UTC));
        ps.setString(l++, row.getProviderId() != null ? row.getProviderId().toString() : ClickHouseUtilsValue.UNKNOWN);
        ps.setString(l, row.getTerminal() != null ? row.getTerminal().toString() : ClickHouseUtilsValue.UNKNOWN);
    }

    @Override
    public int getBatchSize() {
        return batch.size();
    }
}
