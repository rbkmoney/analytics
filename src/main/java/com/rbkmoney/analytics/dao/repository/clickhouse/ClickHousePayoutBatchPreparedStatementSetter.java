package com.rbkmoney.analytics.dao.repository.clickhouse;

import com.rbkmoney.analytics.constant.ClickHouseUtilsValue;
import com.rbkmoney.analytics.dao.model.PayoutRow;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;

@RequiredArgsConstructor
public class ClickHousePayoutBatchPreparedStatementSetter implements BatchPreparedStatementSetter {

    public static final String INSERT = "INSERT INTO analytic.events_sink_payout " +
            "(payoutId, status, payoutType, statusCancelledDetails, isCancelledAfterBeingPaid, timestamp, eventTime, eventTimeHour, " +
            "payoutTime, shopId, partyId, contractId, amount, fee, currency, walletId, accountType, purpose, legalAgreementSignedAt, " +
            "legalAgreementId, legalAgreementValidUntil, russianAccount, russianBankName, russianBankPostAccount, russianBankBik, " +
            "russianInn, internationalAccountHolder, internationalBankName, internationalBankAddress, internationalIban, " +
            "internationalBic, internationalLocalBankCode, internationalLegalEntityLegalName, internationalLegalEntityTradingName, " +
            "internationalLegalEntityRegisteredAddress, internationalLegalEntityActualAddress, internationalLegalEntityRegisteredNumber, " +
            "internationalBankNumber, internationalBankAbaRtn, internationalBankCountryCode, internationalCorrespondentBankNumber, " +
            "internationalCorrespondentBankAccount, internationalCorrespondentBankName, internationalCorrespondentBankAddress, " +
            "internationalCorrespondentBankBic, internationalCorrespondentBankIban, internationalCorrespondentBankAbaRtn, " +
            "internationalCorrespondentBankCountryCode)" +
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " +
            "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private final List<PayoutRow> batch;

    @Override
    public void setValues(PreparedStatement ps, int i) throws SQLException {
        PayoutRow row = batch.get(i);
        int l = 1;

        ps.setString(l++, row.getPayoutId());
        ps.setString(l++, row.getStatus().name());
        ps.setString(l++, row.getPayoutType().name());
        ps.setString(l++, row.getStatusCancelledDetails());
        ps.setBoolean(l++, row.isCancelledAfterBeingPaid());

        ps.setObject(l++, row.getEventTime().toLocalDate());
        ps.setLong(l++, row.getEventTime().toEpochSecond(ZoneOffset.UTC));
        ps.setLong(l++, row.getEventTime().toInstant(ZoneOffset.UTC).truncatedTo(ChronoUnit.HOURS).toEpochMilli());
        ps.setLong(l++, row.getPayoutTime().toEpochSecond(ZoneOffset.UTC));

        ps.setString(l++, row.getShopId());
        ps.setString(l++, row.getPartyId());
        ps.setString(l++, row.getContractId());

        ps.setLong(l++, row.getAmount());
        ps.setLong(l++, row.getFee());
        ps.setString(l++, row.getCurrency());

        ps.setString(l++, row.getWalletId());

        ps.setString(l++, row.getAccountType() != null ? row.getAccountType().name() : ClickHouseUtilsValue.UNKNOWN);
        ps.setString(l++, row.getPurpose());
        ps.setLong(l++, Optional.ofNullable(row.getLegalAgreementSignedAt())
                .map(ldt -> ldt.toEpochSecond(ZoneOffset.UTC))
                .orElse(0L));
        ps.setString(l++, row.getLegalAgreementId());
        ps.setLong(l++, Optional.ofNullable(row.getLegalAgreementValidUntil())
                .map(ldt -> ldt.toEpochSecond(ZoneOffset.UTC))
                .orElse(0L));

        ps.setString(l++, row.getRussianAccount());
        ps.setString(l++, row.getRussianBankName());
        ps.setString(l++, row.getRussianBankPostAccount());
        ps.setString(l++, row.getRussianBankBik());
        ps.setString(l++, row.getRussianInn());

        ps.setString(l++, row.getInternationalAccountHolder());
        ps.setString(l++, row.getInternationalBankName());
        ps.setString(l++, row.getInternationalBankAddress());
        ps.setString(l++, row.getInternationalIban());
        ps.setString(l++, row.getInternationalBic());
        ps.setString(l++, row.getInternationalLocalBankCode());
        ps.setString(l++, row.getInternationalLegalEntityLegalName());
        ps.setString(l++, row.getInternationalLegalEntityTradingName());
        ps.setString(l++, row.getInternationalLegalEntityRegisteredAddress());
        ps.setString(l++, row.getInternationalLegalEntityActualAddress());
        ps.setString(l++, row.getInternationalLegalEntityRegisteredNumber());
        ps.setString(l++, row.getInternationalBankNumber());
        ps.setString(l++, row.getInternationalBankAbaRtn());
        ps.setString(l++, row.getInternationalBankCountryCode());
        ps.setString(l++, row.getInternationalCorrespondentBankNumber());
        ps.setString(l++, row.getInternationalCorrespondentBankAccount());
        ps.setString(l++, row.getInternationalCorrespondentBankName());
        ps.setString(l++, row.getInternationalCorrespondentBankAddress());
        ps.setString(l++, row.getInternationalCorrespondentBankBic());
        ps.setString(l++, row.getInternationalCorrespondentBankIban());
        ps.setString(l++, row.getInternationalCorrespondentBankAbaRtn());
        ps.setString(l, row.getInternationalCorrespondentBankCountryCode());
    }

    @Override
    public int getBatchSize() {
        return batch.size();
    }
}
