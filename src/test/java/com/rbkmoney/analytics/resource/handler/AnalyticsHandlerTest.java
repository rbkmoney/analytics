package com.rbkmoney.analytics.resource.handler;

import com.rbkmoney.analytics.config.RawMapperConfig;
import com.rbkmoney.analytics.converter.*;
import com.rbkmoney.analytics.dao.mapper.SplitRowsMapper;
import com.rbkmoney.analytics.dao.mapper.SplitStatusRowsMapper;
import com.rbkmoney.analytics.dao.repository.clickhouse.ClickHousePaymentRepositoryImpl;
import com.rbkmoney.analytics.dao.repository.clickhouse.ClickHouseRefundRepository;
import com.rbkmoney.analytics.repository.ClickHouseAbstractTest;
import com.rbkmoney.analytics.repository.ClickHousePayoutRepositoryTest;
import com.rbkmoney.damsel.analytics.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;

import static org.junit.Assert.assertEquals;


@Slf4j
@RunWith(SpringRunner.class)
@ContextConfiguration(initializers = ClickHousePayoutRepositoryTest.Initializer.class,
        classes = {RawToNumModelConverter.class, RawToSplitNumberConverter.class, RawToSplitStatusConverter.class,
                SplitRowsMapper.class, SplitStatusRowsMapper.class, RawToNamingDistributionConverter.class,
                RawToShopAmountModelConverter.class,
                RawMapperConfig.class, ClickHousePaymentRepositoryImpl.class, ClickHouseRefundRepository.class,
                AnalyticsHandler.class,
                DaoErrorReasonDistributionsToResponseConverter.class,
                DaoErrorCodeDistributionsToResponseConverter.class,
                DaoNamingDistributionsToResponseConverter.class,
                CostToAmountResponseConverter.class, CountModelCountResponseConverter.class,
                GroupedCurAmountToResponseConverter.class, GroupedCurCountToResponseConverter.class,
                ShopAmountToResponseConverter.class})
public class AnalyticsHandlerTest extends ClickHouseAbstractTest {

    public static final String RUB = "RUB";

    @Autowired
    private AnalyticsHandler analyticsHandler;

    private final TimeFilter timeFilterDefault = new TimeFilter()
            .setFromTime("2016-08-10T16:07:18Z")
            .setToTime("2020-01-31T20:59:59.999000Z");

    @Test
    public void getPaymentsToolDistribution() throws TException {
        PaymentToolDistributionResponse paymentsToolDistribution =
                analyticsHandler.getPaymentsToolDistribution(new FilterRequest()
                        .setMerchantFilter(new MerchantFilter()
                                .setPartyId("ca2e9162-eda2-4d17-bbfa-dc5e39b1772a"))
                        .setTimeFilter(timeFilterDefault));
        String bankCard = "bank_card";

        NamingDistribution namingDistr = findByNameNamingDistribution(paymentsToolDistribution, bankCard);
        assertEquals(33.33, namingDistr.getPercents(), 0);

        paymentsToolDistribution = analyticsHandler.getPaymentsToolDistribution(new FilterRequest()
                .setMerchantFilter(new MerchantFilter()
                        .setPartyId("ca2e9162-eda2-4d17-bbfa-dc5e39b1772a")
                        .setShopIds(List.of("ad8b7bfd-0760-4781-a400-51903ee8e502"))
                )
                .setTimeFilter(timeFilterDefault)
        );

        namingDistr = findByNameNamingDistribution(paymentsToolDistribution, bankCard);
        assertEquals(100.00, namingDistr.getPercents(), 0);

        paymentsToolDistribution.validate();
    }

    @NotNull
    private NamingDistribution findByNameNamingDistribution(PaymentToolDistributionResponse paymentsToolDistribution,
                                                            String bankCard) {
        return paymentsToolDistribution.getPaymentToolsDistributions().stream()
                .filter(namingDistribution -> bankCard.equals(namingDistribution.getName()))
                .findFirst()
                .get();
    }

    @Test
    public void getPaymentsAmount() throws TException {
        AmountResponse paymentsAmount = analyticsHandler.getPaymentsAmount(new FilterRequest()
                .setMerchantFilter(new MerchantFilter()
                        .setPartyId("ca2e9162-eda2-4d17-bbfa-dc5e39b1772a")
                        .setShopIds(List.of("ad8b7bfd-0760-4781-a400-51903ee8e502"))
                )
                .setTimeFilter(timeFilterDefault));
        List<CurrencyGroupedAmount> groupsAmount = paymentsAmount.getGroupsAmount();

        CurrencyGroupedAmount rub = groupsAmount.stream()
                .filter(currencyGroupedAmount -> currencyGroupedAmount.getCurrency().equals(RUB))
                .findFirst()
                .get();

        assertEquals(5000L, rub.amount);

        paymentsAmount.validate();
    }

    @Test
    public void getAveragePayment() throws TException {
        AmountResponse paymentsAmount = analyticsHandler.getAveragePayment(new FilterRequest()
                .setMerchantFilter(new MerchantFilter()
                        .setPartyId("ca2e9162-eda2-4d17-bbfa-dc5e39b1772a")
                        .setShopIds(List.of("ad8b7bfd-0760-4781-a400-51903ee8e502"))
                )
                .setTimeFilter(timeFilterDefault));
        List<CurrencyGroupedAmount> groupsAmount = paymentsAmount.getGroupsAmount();

        CurrencyGroupedAmount rub = groupsAmount.stream()
                .filter(currencyGroupedAmount -> currencyGroupedAmount.getCurrency().equals(RUB))
                .findFirst()
                .get();

        assertEquals(5000L, rub.amount);

        paymentsAmount.validate();
    }

    @Test
    public void getPaymentsCount() throws TException {
        CountResponse paymentsCount = analyticsHandler.getPaymentsCount(new FilterRequest()
                .setMerchantFilter(new MerchantFilter()
                        .setPartyId("ca2e9162-eda2-4d17-bbfa-dc5e39b1772a")
                )
                .setTimeFilter(timeFilterDefault));
        List<CurrecyGroupCount> groupsCount = paymentsCount.getGroupsCount();

        CurrecyGroupCount rub = groupsCount.stream()
                .filter(currencyGroupedAmount -> currencyGroupedAmount.getCurrency().equals(RUB))
                .findFirst()
                .get();

        assertEquals(2L, rub.count);

        paymentsCount.validate();
    }

    @Test
    public void getPaymentsErrorDistribution() throws TException {
        ErrorDistributionsResponse paymentsErrorDistribution =
                analyticsHandler.getPaymentsErrorDistribution(new FilterRequest()
                        .setMerchantFilter(new MerchantFilter()
                                .setPartyId("ca2e9162-eda2-4d17-bbfa-dc5e39b1772a")
                        )
                        .setTimeFilter(timeFilterDefault));
        List<NamingDistribution> errorDistributions = paymentsErrorDistribution.getErrorDistributions();

        NamingDistribution namingDistribution = errorDistributions.stream()
                .filter(currencyGroupedAmount -> "card is failed".equals(currencyGroupedAmount.getName()))
                .findFirst()
                .get();

        assertEquals(100.00, namingDistribution.percents, 0.0);

        paymentsErrorDistribution.validate();
    }

    @Test
    public void getPaymentsErrorCodeDistribution() throws TException {
        SubErrorDistributionsResponse paymentsErrorDistribution =
                analyticsHandler.getPaymentsSubErrorDistribution(new FilterRequest()
                        .setMerchantFilter(new MerchantFilter()
                                .setPartyId("ca2e9162-eda2-4d17-bbfa-dc5e39b1772a")
                        )
                        .setTimeFilter(timeFilterDefault));
        List<ErrorDistribution> errorDistributions = paymentsErrorDistribution.getErrorDistributions();

        ErrorDistribution namingDistribution = errorDistributions.stream()
                .filter(currencyGroupedAmount -> "authorization_failed".equals(currencyGroupedAmount.getError().code))
                .findFirst()
                .get();

        assertEquals(100.00, namingDistribution.percents, 0.0);
        assertEquals("rejected_by_issuer", namingDistribution.error.sub_error.code);

        paymentsErrorDistribution.validate();
    }

    @Test
    public void getPaymentsSplitAmount() throws TException {
        SplitAmountResponse paymentsSplitAmount = analyticsHandler.getPaymentsSplitAmount(new SplitFilterRequest()
                .setSplitUnit(SplitUnit.MINUTE)
                .setFilterRequest(new FilterRequest()
                        .setMerchantFilter(new MerchantFilter()
                                .setPartyId("ca2e9162-eda2-4d17-bbfa-dc5e39b1772d"))
                        .setTimeFilter(timeFilterDefault)));
        List<OffsetAmount> rub = findOffsetAmounts(paymentsSplitAmount, RUB);
        assertEquals(3, rub.size());
        assertEquals(1000L, rub.get(0).getAmount());

        paymentsSplitAmount.validate();
    }

    private List<OffsetAmount> findOffsetAmounts(SplitAmountResponse paymentsSplitAmount, String rubCurrency) {
        return paymentsSplitAmount.getGroupedCurrencyAmounts()
                .stream()
                .filter(groupedCurrencyOffsetAmount -> rubCurrency.equals(groupedCurrencyOffsetAmount.getCurrency()))
                .findFirst().get()
                .getOffsetAmounts();
    }

    @Test
    public void getPaymentsSplitCount() throws TException {
        SplitCountResponse paymentsSplitCount = analyticsHandler.getPaymentsSplitCount(new SplitFilterRequest()
                .setSplitUnit(SplitUnit.MINUTE)
                .setFilterRequest(new FilterRequest()
                        .setMerchantFilter(new MerchantFilter()
                                .setPartyId("ca2e9162-eda2-4d17-bbfa-dc5e39b1772d"))
                        .setTimeFilter(timeFilterDefault)));
        List<GroupedStatusOffsetCount> rub = findOffsetCount(paymentsSplitCount, RUB);

        System.out.println(rub);

        paymentsSplitCount.validate();
    }

    private List<GroupedStatusOffsetCount> findOffsetCount(SplitCountResponse paymentsSplitCount, String rubCurrency) {
        return paymentsSplitCount.getPaymentToolsDestrobutions()
                .stream()
                .filter(groupedCurrencyOffsetCount -> rubCurrency.equals(groupedCurrencyOffsetCount.getCurrency()))
                .findFirst().get()
                .getOffsetAmounts();
    }

    @Test
    public void getRefundsAmount() throws TException {
        AmountResponse paymentsAmount = analyticsHandler.getRefundsAmount(new FilterRequest()
                .setMerchantFilter(new MerchantFilter()
                        .setPartyId("ca2e9162-eda2-4d17-bbfa-dc5e39b1772f")
                        .setShopIds(List.of("ad8b7bfd-0760-4781-a400-51903ee8e509"))
                )
                .setTimeFilter(timeFilterDefault));
        List<CurrencyGroupedAmount> groupsAmount = paymentsAmount.getGroupsAmount();

        CurrencyGroupedAmount rub = groupsAmount.stream()
                .filter(currencyGroupedAmount -> currencyGroupedAmount.getCurrency().equals(RUB))
                .findFirst()
                .get();

        assertEquals(5000L, rub.amount);

        paymentsAmount.validate();
    }

    @Test
    public void getRefundsAmountWithExclude() throws TException {
        AmountResponse paymentsAmount = analyticsHandler.getRefundsAmount(new FilterRequest()
                .setMerchantFilter(new MerchantFilter()
                        .setPartyId("ca2e9162-eda2-4d17-bbfa-dc5e39b1772f")
                        .setExcludeShopIds(List.of("ad8b7bfd-0760-4781-a400-51903ee8e509"))
                )
                .setTimeFilter(timeFilterDefault));
        List<CurrencyGroupedAmount> groupsAmount = paymentsAmount.getGroupsAmount();

        Assert.assertTrue(groupsAmount.isEmpty());
    }

    @Test
    public void getCurrentBalances() throws TException {
        AmountResponse paymentsAmount = analyticsHandler.getCurrentBalances(new MerchantFilter()
                .setPartyId("ca2e9162-eda2-4d17-bbfa-dc5e39b1772f")
                .setShopIds(List.of("ad8b7bfd-0760-4781-a400-51903ee8e509"))
        );
        List<CurrencyGroupedAmount> groupsAmount = paymentsAmount.getGroupsAmount();

        CurrencyGroupedAmount rub = groupsAmount.stream()
                .filter(currencyGroupedAmount -> currencyGroupedAmount.getCurrency().equals(RUB))
                .findFirst()
                .get();

        assertEquals(44900L, rub.amount);

        paymentsAmount.validate();
    }

    @Test
    public void getShopBalances() throws TException {
        ShopAmountResponse paymentsAmount = analyticsHandler.getCurrentShopBalances(new MerchantFilter()
                .setPartyId("ca2e9162-eda2-4d17-bbfa-dc5e39b1772f")
                .setShopIds(List.of("ad8b7bfd-0760-4781-a400-51903ee8e509"))
        );
        List<ShopGroupedAmount> groupsAmount = paymentsAmount.getGroupsAmount();

        ShopGroupedAmount shopGroupedAmount = groupsAmount.stream()
                .filter(currencyGroupedAmount -> currencyGroupedAmount.getShopId()
                        .equals("ad8b7bfd-0760-4781-a400-51903ee8e509"))
                .findFirst()
                .get();

        assertEquals(44900L, shopGroupedAmount.amount);
        assertEquals("ad8b7bfd-0760-4781-a400-51903ee8e509", shopGroupedAmount.shop_id);
        assertEquals(RUB, shopGroupedAmount.currency);

        paymentsAmount.validate();
    }

}
