package com.rbkmoney.analytics.repository;

import com.rbkmoney.analytics.config.RawMapperConfig;
import com.rbkmoney.analytics.constant.PayoutAccountType;
import com.rbkmoney.analytics.constant.PayoutStatus;
import com.rbkmoney.analytics.constant.PayoutType;
import com.rbkmoney.analytics.converter.*;
import com.rbkmoney.analytics.dao.mapper.SplitRowsMapper;
import com.rbkmoney.analytics.dao.mapper.SplitStatusRowsMapper;
import com.rbkmoney.analytics.dao.model.PayoutRow;
import com.rbkmoney.analytics.dao.repository.clickhouse.ClickHousePayoutRepository;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import java.time.LocalDateTime;
import java.util.List;

import static org.junit.Assert.assertEquals;

@Slf4j
@RunWith(SpringRunner.class)
@ContextConfiguration(
        initializers = ClickHousePayoutRepositoryTest.Initializer.class,
        classes = {RawToNumModelConverter.class, RawToSplitNumberConverter.class, RawToSplitStatusConverter.class,
                SplitRowsMapper.class, SplitStatusRowsMapper.class, RawToNamingDistributionConverter.class,
                RawToShopAmountModelConverter.class,
                RawMapperConfig.class, ClickHousePayoutRepository.class})
public class ClickHousePayoutRepositoryTest extends ClickHouseAbstractTest {

    @Autowired
    private JdbcTemplate clickHouseJdbcTemplate;

    @Autowired
    private ClickHousePayoutRepository clickHousePayoutRepository;

    @Test
    public void shouldSavePayouts() {
        // Given
        PayoutRow row = new PayoutRow();
        row.setPayoutId("payoutId");
        row.setStatus(PayoutStatus.paid);
        row.setPayoutType(PayoutType.bank_account);
        row.setCancelledAfterBeingPaid(false);
        row.setEventTime(LocalDateTime.now());
        row.setPayoutTime(LocalDateTime.now());
        row.setShopId("shopId");
        row.setPartyId("partyId");
        row.setContractId("contractId");
        row.setAmount(10000L);
        row.setFee(1000L);
        row.setCurrency("RUB");
        row.setWalletId("walletId");
        row.setAccountType(PayoutAccountType.russian_payout_account);
        row.setLegalAgreementId("id");
        row.setLegalAgreementSignedAt(LocalDateTime.now());
        row.setRussianAccount("rus");
        row.setRussianBankName("rus");
        row.setRussianBankPostAccount("rus");
        row.setRussianBankBik("rus");
        row.setRussianInn("rus");

        // When
        clickHousePayoutRepository.insertBatch(List.of(row));

        // Then
        long sum = clickHouseJdbcTemplate.queryForObject(
                "SELECT shopId, sum(amount) as sum " +
                        "from analytic.events_sink_payout where status = 'paid' " +
                        "group by partyId, shopId, currency " +
                        "having shopId = 'shopId' and currency = 'RUB'",
                (resultSet, i) -> resultSet.getLong("sum"));

        assertEquals(10000, sum);
    }
}
