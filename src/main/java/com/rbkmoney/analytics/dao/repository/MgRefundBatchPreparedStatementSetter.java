package com.rbkmoney.analytics.dao.repository;

import com.rbkmoney.analytics.dao.model.MgRefundRow;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

@RequiredArgsConstructor
public class MgRefundBatchPreparedStatementSetter implements BatchPreparedStatementSetter {

    public static final String INSERT = "INSERT INTO analytic.events_sink_refund " +
            "(timestamp, eventTime, eventTimeHour, partyId, shopId, email, " +
            "totalAmount, merchantAmount, guaranteeDeposit, systemFee, providerFee, externalFee, currency, providerName, " +
            "status, errorReason,  invoiceId, " +
            "paymentId, refundId, sequenceId, ip, " +
            "fingerprint,cardToken, paymentSystem, digitalWalletProvider, digitalWalletToken, cryptoCurrency, mobileOperator)" +
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private final List<MgRefundRow> batch;

    @Override
    public void setValues(PreparedStatement ps, int i) throws SQLException {
        MgRefundRow row = batch.get(i);
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
        ps.setString(l++, row.getRefundId());
        ps.setLong(l++, row.getSequenceId());

        ps.setString(l++, row.getIp());

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
