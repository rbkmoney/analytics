package com.rbkmoney.analytics.dao.repository;

import com.rbkmoney.analytics.constant.ClickhouseUtilsValue;
import com.rbkmoney.analytics.dao.model.MgPaymentSinkRow;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

@RequiredArgsConstructor
public class MgPaymentBatchPreparedStatementSetter implements BatchPreparedStatementSetter {

    public static final String INSERT = "INSERT INTO analytic.events_sink " +
            "(timestamp, eventTime, eventTimeHour, partyId, shopId, email," +
            "totalAmount, merchantAmount, guaranteeDeposit, systemFee, providerFee, externalFee, currency, providerName, " +
            "status, errorReason,  invoiceId, " +
            "paymentId, sequenceId, ip, bin, maskedPan, paymentTool, " +
            "fingerprint,cardToken, paymentSystem, digitalWalletProvider, digitalWalletToken, cryptoCurrency, mobileOperator)" +
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

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

        ps.setLong(l++, row.getTotalAmount());
        ps.setLong(l++, row.getMerchantAmount());
        ps.setLong(l++, row.getGuaranteeDeposit());
        ps.setLong(l++, row.getSystemFee());
        ps.setLong(l++, row.getExternalFee());
        ps.setLong(l++, row.getProviderFee());
        ps.setString(l++, row.getCurrency());

        ps.setString(l++, row.getProvider());

        ps.setString(l++, row.getStatus().name());

        ps.setString(l++, row.getErrorCode());

        ps.setString(l++, row.getInvoiceId());
        ps.setString(l++, row.getPaymentId());
        ps.setLong(l++, row.getSequenceId());

        ps.setString(l++, row.getIp());
        ps.setString(l++, row.getBin());
        ps.setString(l++, row.getMaskedPan());
        ps.setString(l++, row.getPaymentTool() != null ? row.getPaymentTool().name() : ClickhouseUtilsValue.UNKNOWN);

        ps.setString(l++, row.getFingerprint());
        ps.setString(l++, row.getCardToken());
        ps.setString(l++, row.getPaymentSystem());
        ps.setString(l++, row.getDigitalWalletProvider());
        ps.setString(l++, row.getDigitalWalletToken());
        ps.setString(l++, row.getCryptoCurrency());
        ps.setString(l, row.getMobileOperator());

    }

    @Override
    public int getBatchSize() {
        return batch.size();
    }
}
