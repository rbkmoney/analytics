package com.rbkmoney.analytics.resource.handler;

import com.rbkmoney.analytics.config.RawMapperConfig;
import com.rbkmoney.analytics.converter.*;
import com.rbkmoney.analytics.dao.mapper.SplitRowsMapper;
import com.rbkmoney.analytics.dao.mapper.SplitStatusRowsMapper;
import com.rbkmoney.analytics.dao.repository.clickhouse.ClickHousePaymentRepository;
import com.rbkmoney.analytics.dao.repository.clickhouse.ClickHouseRefundRepository;
import com.rbkmoney.analytics.repository.ClickHouseAbstractTest;
import com.rbkmoney.analytics.repository.PaymentRepositoryTest;
import com.rbkmoney.damsel.analytics.*;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;


@Slf4j
@RunWith(SpringRunner.class)
@ContextConfiguration(initializers = PaymentRepositoryTest.Initializer.class,
        classes = {RawToNumModelConverter.class, RawToSplitNumberConverter.class, RawToSplitStatusConverter.class,
                SplitRowsMapper.class, SplitStatusRowsMapper.class, RawToNamingDistributionConverter.class,
                RawMapperConfig.class, ClickHousePaymentRepository.class, ClickHouseRefundRepository.class, AnalyticsHandler.class,
                DaoErrorDistributionsToResponseConverter.class, DaoNamingDistributionsToResponseConverter.class,
                CostToAmountResponseConverter.class, CountModelCountResponseConverter.class,
                GroupedCurAmountToResponseConverter.class, GroupedCurCountToResponseConverter.class})
public class AnalyticsHandlerTest extends ClickHouseAbstractTest {

    @Autowired
    private AnalyticsHandler analyticsHandler;

    @Test
    public void getPaymentsToolDistribution() {
        PaymentToolDistributionResponse paymentsToolDistribution = analyticsHandler.getPaymentsToolDistribution(new FilterRequest()
                .setMerchantFilter(new MerchantFilter()
                        .setPartyId("ca2e9162-eda2-4d17-bbfa-dc5e39b1772a"))
                .setTimeFilter(new TimeFilter()
                        .setFromTime("2016-08-10T16:07:18Z")
                        .setToTime("2020-08-10T16:07:18Z")
                )
        );
        String bankCard = "bank_card";

        NamingDistribution namingDistr = findByNameNamingDistribution(paymentsToolDistribution, bankCard);
        Assert.assertEquals(33L, namingDistr.getPercents());

        paymentsToolDistribution = analyticsHandler.getPaymentsToolDistribution(new FilterRequest()
                .setMerchantFilter(new MerchantFilter()
                        .setPartyId("ca2e9162-eda2-4d17-bbfa-dc5e39b1772a")
                        .setShopIds(List.of("ad8b7bfd-0760-4781-a400-51903ee8e502"))
                )
                .setTimeFilter(new TimeFilter()
                        .setFromTime("2016-08-10T16:07:18Z")
                        .setToTime("2020-08-10T16:07:18Z")
                )
        );

        namingDistr = findByNameNamingDistribution(paymentsToolDistribution, bankCard);
        Assert.assertEquals(100L, namingDistr.getPercents());
    }

    @NotNull
    private NamingDistribution findByNameNamingDistribution(PaymentToolDistributionResponse paymentsToolDistribution, String bankCard) {
        return paymentsToolDistribution.getPaymentToolsDistributions().stream()
                .filter(namingDistribution -> {
                    return bankCard.equals(namingDistribution.getName());
                })
                .findFirst().get();
    }

    @Test
    public void getPaymentsAmount() {
        AmountResponse paymentsAmount = analyticsHandler.getPaymentsAmount(new FilterRequest()
                .setMerchantFilter(new MerchantFilter()
                        .setPartyId("ca2e9162-eda2-4d17-bbfa-dc5e39b1772a")
                        .setShopIds(List.of("ad8b7bfd-0760-4781-a400-51903ee8e502"))
                )
                .setTimeFilter(new TimeFilter()
                        .setFromTime("2016-08-10T16:07:18Z")
                        .setToTime("2020-08-10T16:07:18Z")
                ));
        String RUB = "RUB";
        List<CurrencyGroupedAmount> groupsAmount = paymentsAmount.getGroupsAmount();

        CurrencyGroupedAmount rub = groupsAmount.stream()
                .filter(currencyGroupedAmount -> currencyGroupedAmount.getCurrency().equals("RUB"))
                .findFirst()
                .get();

        Assert.assertEquals(5000L, rub.amount);
    }

    @Test
    public void getAveragePayment() {
        AmountResponse paymentsAmount = analyticsHandler.getAveragePayment(new FilterRequest()
                .setMerchantFilter(new MerchantFilter()
                        .setPartyId("ca2e9162-eda2-4d17-bbfa-dc5e39b1772a")
                        .setShopIds(List.of("ad8b7bfd-0760-4781-a400-51903ee8e502"))
                )
                .setTimeFilter(new TimeFilter()
                        .setFromTime("2016-08-10T16:07:18Z")
                        .setToTime("2020-08-10T16:07:18Z")
                ));
        String RUB = "RUB";
        List<CurrencyGroupedAmount> groupsAmount = paymentsAmount.getGroupsAmount();

        CurrencyGroupedAmount rub = groupsAmount.stream()
                .filter(currencyGroupedAmount -> currencyGroupedAmount.getCurrency().equals("RUB"))
                .findFirst()
                .get();

        Assert.assertEquals(5000L, rub.amount);
    }

    @Test
    public void getPaymentsCount() {
        CountResponse paymentsCount = analyticsHandler.getPaymentsCount(new FilterRequest()
                .setMerchantFilter(new MerchantFilter()
                        .setPartyId("ca2e9162-eda2-4d17-bbfa-dc5e39b1772a")
                )
                .setTimeFilter(new TimeFilter()
                        .setFromTime("2016-08-10T16:07:18Z")
                        .setToTime("2020-08-10T16:07:18Z")
                ));
        String RUB = "RUB";
        List<CurrecyGroupCount> groupsCount = paymentsCount.getGroupsCount();

        CurrecyGroupCount rub = groupsCount.stream()
                .filter(currencyGroupedAmount -> currencyGroupedAmount.getCurrency().equals("RUB"))
                .findFirst()
                .get();

        Assert.assertEquals(3L, rub.count);
    }

    @Test
    public void getPaymentsErrorDistribution() {
        ErrorDistributionsResponse paymentsErrorDistribution = analyticsHandler.getPaymentsErrorDistribution(new FilterRequest()
                .setMerchantFilter(new MerchantFilter()
                        .setPartyId("ca2e9162-eda2-4d17-bbfa-dc5e39b1772a")
                )
                .setTimeFilter(new TimeFilter()
                        .setFromTime("2016-08-10T16:07:18Z")
                        .setToTime("2020-08-10T16:07:18Z")
                ));
        List<NamingDistribution> errorDistributions = paymentsErrorDistribution.getErrorDistributions();

        NamingDistribution namingDistribution = errorDistributions.stream()
                .filter(currencyGroupedAmount -> "card is failed".equals(currencyGroupedAmount.getName()))
                .findFirst()
                .get();

        Assert.assertEquals(100L, namingDistribution.percents);
    }

    @Test
    public void getPaymentsSplitAmount() {
        SplitAmountResponse paymentsSplitAmount = analyticsHandler.getPaymentsSplitAmount(new SplitFilterRequest()
                .setSplitUnit(SplitUnit.MINUTE)
                .setFilterRequest(new FilterRequest()
                        .setMerchantFilter(new MerchantFilter()
                                .setPartyId("ca2e9162-eda2-4d17-bbfa-dc5e39b1772d"))
                        .setTimeFilter(new TimeFilter()
                                .setFromTime("2016-08-10T16:07:18Z")
                                .setToTime("2020-08-10T16:07:18Z"))));
        String RUB = "RUB";
        List<OffsetAmount> rub = findOffsetAmounts(paymentsSplitAmount, RUB);
        Assert.assertEquals(3, rub.size());
        Assert.assertEquals(1000L, rub.get(0).getAmount());
    }

    private List<OffsetAmount> findOffsetAmounts(SplitAmountResponse paymentsSplitAmount, String RUB) {
        return paymentsSplitAmount.getGroupedCurrencyAmounts()
                .stream()
                .filter(groupedCurrencyOffsetAmount -> {
                    return RUB.equals(groupedCurrencyOffsetAmount.getCurrency());
                })
                .findFirst().get()
                .getOffsetAmounts();
    }

    @Test
    public void getPaymentsSplitCount() {
    }

    @Test
    public void getRefundsAmount() {
    }
}