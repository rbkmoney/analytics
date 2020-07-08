package com.rbkmoney.analytics.dao.repository.clickhouse;

import com.rbkmoney.analytics.dao.model.ContractorRow;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;

@RequiredArgsConstructor
public class ClickHouseContractorBatchPreparedStatementSetter implements BatchPreparedStatementSetter {

    public static final String INSERT = "INSERT INTO analytic.events_sink_contractor (timestamp, eventTime, eventTimeHour," +
            " partyId, contractorId, contractorType, regUserEmail, legalEntityType, russianLegalEntityName, russianLegalEntityRegisteredNumber," +
            " russianLegalEntityInn, russianLegalEntityActualAddress, russianLegalEntityPostAddress, russianLegalEntityRepresentativePosition, russianLegalEntityRepresentativeFullName," +
            " russianLegalEntityRepresentativeDocument, russianLegalEntityBankAccount, russianLegalEntityBankName, russianLegalEntityBankPostAccount," +
            " russianLegalEntityBankBik, internationalLegalEntityName, internationalLegalEntityTradingName, internationalLegalEntityRegisteredAddress," +
            " internationalLegalEntityRegisteredNumber, privateEntityType, russianPrivateEntityFirstName, russianPrivateEntitySecondName, russianPrivateEntityMiddleName, russianPrivateEntityPhoneNumber," +
            " russianPrivateEntityEmail, contractorIdentificationLevel)" +
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";


    private final List<ContractorRow> batch;

    @Override
    public void setValues(PreparedStatement ps, int i) throws SQLException {
        ContractorRow row = batch.get(i);
        int l = 1;

        ps.setObject(l++, row.getEventTime().toLocalDate());
        ps.setLong(l++, row.getEventTime().toEpochSecond(ZoneOffset.UTC));
        ps.setLong(l++, row.getEventTime().toInstant(ZoneOffset.UTC).truncatedTo(ChronoUnit.HOURS).toEpochMilli());
        ps.setString(l++, row.getPartyId());
        ps.setString(l++, row.getContractorId());
        ps.setString(l++, row.getContractorType().name());
        ps.setString(l++, row.getRegUserEmail());
        ps.setString(l++, row.getLegalEntityType() != null ? row.getLegalEntityType().name() : null);
        ps.setString(l++, row.getRussianLegalEntityName());
        ps.setString(l++, row.getRussianLegalEntityRegisteredNumber());
        ps.setString(l++, row.getRussianLegalEntityInn());
        ps.setString(l++, row.getRussianLegalEntityActualAddress());
        ps.setString(l++, row.getRussianLegalEntityPostAddress());
        ps.setString(l++, row.getRussianLegalEntityRepresentativePosition());
        ps.setString(l++, row.getRussianLegalEntityRepresentativeFullName());
        ps.setString(l++, row.getRussianLegalEntityRepresentativeDocument());
        ps.setString(l++, row.getRussianLegalEntityBankAccount());
        ps.setString(l++, row.getRussianLegalEntityBankName());
        ps.setString(l++, row.getRussianLegalEntityBankPostAccount());
        ps.setString(l++, row.getRussianLegalEntityBankBik());
        ps.setString(l++, row.getInternationalLegalEntityName());
        ps.setString(l++, row.getInternationalLegalEntityTradingName());
        ps.setString(l++, row.getInternationalLegalEntityRegisteredAddress());
        ps.setString(l++, row.getInternationalLegalEntityRegisteredNumber());
        ps.setString(l++, row.getPrivateEntityType() != null ? row.getPrivateEntityType().name() : null);
        ps.setString(l++, row.getRussianPrivateEntityFirstName());
        ps.setString(l++, row.getRussianPrivateEntitySecondName());
        ps.setString(l++, row.getRussianPrivateEntityMiddleName());
        ps.setString(l++, row.getRussianPrivateEntityPhoneNumber());
        ps.setString(l++, row.getRussianPrivateEntityEmail());
        ps.setString(l, row.getContractorIdentificationLevel() != null ? row.getContractorIdentificationLevel().name() : null);
    }

    @Override
    public int getBatchSize() {
        return batch.size();
    }
}
