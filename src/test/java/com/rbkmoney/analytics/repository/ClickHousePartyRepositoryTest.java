package com.rbkmoney.analytics.repository;

import com.rbkmoney.analytics.config.RawMapperConfig;
import com.rbkmoney.analytics.constant.*;
import com.rbkmoney.analytics.converter.RawToNamingDistributionConverter;
import com.rbkmoney.analytics.converter.RawToNumModelConverter;
import com.rbkmoney.analytics.converter.RawToSplitNumberConverter;
import com.rbkmoney.analytics.converter.RawToSplitStatusConverter;
import com.rbkmoney.analytics.dao.mapper.SplitRowsMapper;
import com.rbkmoney.analytics.dao.mapper.SplitStatusRowsMapper;
import com.rbkmoney.analytics.dao.model.ContractorRow;
import com.rbkmoney.analytics.dao.model.PartyRow;
import com.rbkmoney.analytics.dao.model.ShopRow;
import com.rbkmoney.analytics.dao.repository.clickhouse.ClickHouseContractorRepository;
import com.rbkmoney.analytics.dao.repository.clickhouse.ClickHousePartyRepository;
import com.rbkmoney.analytics.dao.repository.clickhouse.ClickHousePayoutRepository;
import com.rbkmoney.analytics.dao.repository.clickhouse.ClickHouseShopRepository;
import com.rbkmoney.geck.common.util.TypeUtil;
import io.github.benas.randombeans.api.EnhancedRandom;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import java.sql.Timestamp;
import java.time.*;
import java.util.Collections;

@Slf4j
@RunWith(SpringRunner.class)
@ContextConfiguration(
        initializers = ClickHousePayoutRepositoryTest.Initializer.class,
        classes = {RawToNumModelConverter.class, RawToSplitNumberConverter.class, RawToSplitStatusConverter.class,
                SplitRowsMapper.class, SplitStatusRowsMapper.class, RawToNamingDistributionConverter.class,
                RawMapperConfig.class, ClickHousePartyRepository.class, ClickHouseShopRepository.class, ClickHouseContractorRepository.class})
public class ClickHousePartyRepositoryTest extends ClickHouseAbstractTest {

    @Autowired
    private JdbcTemplate clickHouseJdbcTemplate;

    @Autowired
    private ClickHousePartyRepository clickHousePartyRepository;

    @Autowired
    private ClickHouseShopRepository clickHouseShopRepository;

    @Autowired
    private ClickHouseContractorRepository clickHouseContractorRepository;

    @Test
    public void testPartySave() {
        PartyRow partyRow = EnhancedRandom.random(PartyRow.class);
        clickHousePartyRepository.insertBatch(Collections.singletonList(partyRow));
        PartyRow savedRow = clickHouseJdbcTemplate.queryForObject(
                String.format("SELECT partyId, email, blocking, suspension" +
                        " FROM analytic.events_sink_party WHERE email = '%s'", partyRow.getEmail()),
                (resultSet, i) -> {
                    PartyRow row = new PartyRow();
                    row.setPartyId(resultSet.getString("partyId"));
                    row.setEmail(resultSet.getString("email"));
                    row.setBlocking(BlockingType.valueOf(resultSet.getString("blocking")));
                    row.setSuspension(SuspensionType.valueOf(resultSet.getString("suspension")));

                    return row;
                });

        Assert.assertEquals(partyRow.getPartyId(), savedRow.getPartyId());
        Assert.assertEquals(partyRow.getEmail(), savedRow.getEmail());
        Assert.assertEquals(partyRow.getBlocking(), savedRow.getBlocking());
        Assert.assertEquals(partyRow.getSuspension(), savedRow.getSuspension());
    }

    @Test
    public void testShopSave() {
        ShopRow shopRow = EnhancedRandom.random(ShopRow.class);
        clickHouseShopRepository.insertBatch(Collections.singletonList(shopRow));
        ShopRow savedRow = clickHouseJdbcTemplate.queryForObject(
                String.format("SELECT partyId, shopId, blocking, suspension" +
                        " FROM analytic.events_sink_shop WHERE partyId = '%s'", shopRow.getPartyId()),
                (resultSet, i) -> {
                    ShopRow row = new ShopRow();
                    row.setPartyId(resultSet.getString("partyId"));
                    row.setShopdId(resultSet.getString("shopId"));
                    row.setBlocking(BlockingType.valueOf(resultSet.getString("blocking")));
                    row.setSuspension(SuspensionType.valueOf(resultSet.getString("suspension")));

                    return row;
                });
        Assert.assertEquals(shopRow.getPartyId(), savedRow.getPartyId());
        Assert.assertEquals(shopRow.getShopdId(), savedRow.getShopdId());
        Assert.assertEquals(shopRow.getBlocking(), savedRow.getBlocking());
        Assert.assertEquals(shopRow.getSuspension(), savedRow.getSuspension());
    }

    @Test
    public void testContractorSave() {
        ContractorRow contractorRow = EnhancedRandom.random(ContractorRow.class);
        clickHouseContractorRepository.insertBatch(Collections.singletonList(contractorRow));
        ContractorRow savedRow = clickHouseJdbcTemplate.queryForObject(
                String.format("SELECT partyId, contractorType, legalEntityType, privateEntityType" +
                        " FROM analytic.events_sink_contractor WHERE partyId = '%s'", contractorRow.getPartyId()),
                (resultSet, i) -> {
                    ContractorRow row = new ContractorRow();
                    row.setPartyId(resultSet.getString("partyId"));
                    row.setContractorType(ContractorType.valueOf(resultSet.getString("contractorType")));
                    row.setLegalEntityType(LegalEntityType.valueOf(resultSet.getString("legalEntityType")));
                    row.setPrivateEntityType(PrivateEntityType.valueOf(resultSet.getString("privateEntityType")));

                    return row;
                });
        Assert.assertEquals(contractorRow.getPartyId(), savedRow.getPartyId());
        Assert.assertEquals(contractorRow.getContractorType(), savedRow.getContractorType());
        Assert.assertEquals(contractorRow.getLegalEntityType(), savedRow.getLegalEntityType());
        Assert.assertEquals(contractorRow.getPrivateEntityType(), savedRow.getPrivateEntityType());
    }

}
