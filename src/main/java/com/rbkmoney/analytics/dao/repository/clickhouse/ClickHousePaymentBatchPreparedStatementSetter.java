package com.rbkmoney.analytics.dao.repository.clickhouse;

import com.rbkmoney.analytics.constant.ClickHouseUtilsValue;
import com.rbkmoney.analytics.dao.model.MgPaymentSinkRow;
import com.rbkmoney.analytics.domain.CashFlowResult;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

@RequiredArgsConstructor
public class ClickHousePaymentBatchPreparedStatementSetter implements BatchPreparedStatementSetter {

    public static final String INSERT = "INSERT INTO analytic.events_sink " +
            "(timestamp, eventTime, eventTimeHour, partyId, shopId, email," +
            "amount, guaranteeDeposit, systemFee, providerFee, externalFee, currency, providerName, " +
            "status, errorCode, errorReason,  invoiceId, paymentId, sequenceId, ip, bin, maskedPan, paymentTool, " +
            "fingerprint,cardToken, paymentSystem, digitalWalletProvider, digitalWalletToken, cryptoCurrency, mobileOperator," +
            "paymentCountry, bankCountry)" +
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private final List<MgPaymentSinkRow> batch;

    @Override
    public void setValues(PreparedStatement ps, int i) throws SQLException {
        MgPaymentSinkRow row = batch.get(i);
        int l = 1;
        ps.setDate(l++, row.getTimestamp());
        ps.setLong(l++, row.getEventTime());
        ps.setLong(l++, row.getEventTimeHour());

        ps.setString(l++, row.getPartyId());
        ps.setString(l++, row.getShopId());

        ps.setString(l++, row.getEmail());

        CashFlowResult cashFlowResult = row.getCashFlowResult();
        if (cashFlowResult != null) {
            ps.setLong(l++, cashFlowResult.getAmount());
            ps.setLong(l++, cashFlowResult.getGuaranteeDeposit());
            ps.setLong(l++, cashFlowResult.getSystemFee());
            ps.setLong(l++, cashFlowResult.getExternalFee());
            ps.setLong(l++, cashFlowResult.getProviderFee());
        }
        ps.setString(l++, row.getCurrency());

        ps.setString(l++, row.getProvider());

        ps.setString(l++, row.getStatus().name());

        ps.setString(l++, row.getErrorCode());
        ps.setString(l++, row.getErrorReason());

        ps.setString(l++, row.getInvoiceId());
        ps.setString(l++, row.getPaymentId());
        ps.setLong(l++, row.getSequenceId());

        ps.setString(l++, row.getIp());
        ps.setString(l++, row.getBin());
        ps.setString(l++, row.getMaskedPan());
        ps.setString(l++, row.getPaymentTool() != null ? row.getPaymentTool().name() : ClickHouseUtilsValue.UNKNOWN);

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
