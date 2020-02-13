package com.rbkmoney.analytics.repository;

import com.rbkmoney.analytics.config.RawMapperConfig;
import com.rbkmoney.analytics.converter.RawToNamingDistributionConverter;
import com.rbkmoney.analytics.converter.RawToNumModelConverter;
import com.rbkmoney.analytics.converter.RawToSplitNumberConverter;
import com.rbkmoney.analytics.converter.RawToSplitStatusConverter;
import com.rbkmoney.analytics.dao.mapper.SplitRowsMapper;
import com.rbkmoney.analytics.dao.mapper.SplitStatusRowsMapper;
import com.rbkmoney.analytics.dao.model.NamingDistribution;
import com.rbkmoney.analytics.dao.model.NumberModel;
import com.rbkmoney.analytics.dao.model.SplitNumberModel;
import com.rbkmoney.analytics.dao.model.SplitStatusNumberModel;
import com.rbkmoney.analytics.dao.repository.MgPaymentRepository;
import com.rbkmoney.damsel.analytics.SplitUnit;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;
import java.util.Optional;

@Slf4j
@RunWith(SpringRunner.class)
@ContextConfiguration(initializers = PaymentRepositoryTest.Initializer.class,
        classes = {RawToNumModelConverter.class, RawToSplitNumberConverter.class, RawToSplitStatusConverter.class,
                SplitRowsMapper.class, SplitStatusRowsMapper.class, RawToNamingDistributionConverter.class, RawMapperConfig.class, MgPaymentRepository.class})
public class PaymentRepositoryTest extends ClickhouseAbstractTest {

    public static final String RUB = "RUB";
    @Autowired
    private MgPaymentRepository mgPaymentRepository;

    @Test
    public void testAmountListPayment() {
        List<NumberModel> numberModels = mgPaymentRepository.getPaymentsAmount("ca2e9162-eda2-4d17-bbfa-dc5e39b1772a", null, 1575554400000L, 1575556887697L);

        NumberModel numberModel = findCost(numberModels, RUB);

        Assert.assertEquals(6000L, numberModel.getNumber().longValue());
        numberModels = mgPaymentRepository.getPaymentsAmount("ca2e9162-eda2-4d17-bbfa-dc5e39b1772a", List.of("ad8b7bfd-0760-4781-a400-51903ee8e502"), 1575554400000L, 1575556887697L);

        numberModel = findCost(numberModels, RUB);

        Assert.assertEquals(5000L, numberModel.getNumber().longValue());
    }

    @Test
    public void testSplitAmountListPayment() {
        List<SplitNumberModel> costs = mgPaymentRepository.getPaymentsSplitAmount("ca2e9162-eda2-4d17-bbfa-dc5e39b1772d", null, 1573554400000L, 1579666000698L, SplitUnit.MINUTE);
        Assert.assertEquals(3, costs.size());
        NumberModel numberModel = findCost(costs, RUB);
        Assert.assertEquals(1000L, numberModel.getNumber().longValue());

        costs = mgPaymentRepository.getPaymentsSplitAmount("ca2e9162-eda2-4d17-bbfa-dc5e39b1772d", null, 1573554400000L, 1579666000698L, SplitUnit.HOUR);
        Assert.assertEquals(3, costs.size());
        numberModel = findCost(costs, RUB);
        Assert.assertEquals(1000L, numberModel.getNumber().longValue());

        costs = mgPaymentRepository.getPaymentsSplitAmount("ca2e9162-eda2-4d17-bbfa-dc5e39b1772d", null, 1573554400000L, 1579666000698L, SplitUnit.DAY);
        Assert.assertEquals(3, costs.size());
        numberModel = findCost(costs, RUB);
        Assert.assertEquals(1000L, numberModel.getNumber().longValue());

        costs = mgPaymentRepository.getPaymentsSplitAmount("ca2e9162-eda2-4d17-bbfa-dc5e39b1772d", null, 1573554400000L, 1579666000698L, SplitUnit.MONTH);
        numberModel = findCost(costs, RUB);
        Assert.assertEquals(3000L, numberModel.getNumber().longValue());

        costs = mgPaymentRepository.getPaymentsSplitAmount("ca2e9162-eda2-4d17-bbfa-dc5e39b1772d", null, 1573554400000L, 1579666000698L, SplitUnit.YEAR);
        numberModel = findCost(costs, RUB);
        Assert.assertEquals(3000L, numberModel.getNumber().longValue());
    }


    @Test
    public void testSplitCpountListPayment() {
        List<SplitStatusNumberModel> costs = mgPaymentRepository.getPaymentsSplitCount("ca2e9162-eda2-4d17-bbfa-dc5e39b1772d", null, 1573554400000L, 1579666000698L, SplitUnit.MINUTE);
        Assert.assertEquals(3, costs.size());
        NumberModel numberModel = findCost(costs, RUB);
        Assert.assertEquals(1, numberModel.getNumber().longValue());

        costs = mgPaymentRepository.getPaymentsSplitCount("ca2e9162-eda2-4d17-bbfa-dc5e39b1772d", null, 1573554400000L, 1579666000698L, SplitUnit.HOUR);
        Assert.assertEquals(3, costs.size());
        numberModel = findCost(costs, RUB);
        Assert.assertEquals(1, numberModel.getNumber().longValue());

        costs = mgPaymentRepository.getPaymentsSplitCount("ca2e9162-eda2-4d17-bbfa-dc5e39b1772d", null, 1573554400000L, 1579666000698L, SplitUnit.DAY);
        Assert.assertEquals(3, costs.size());
        numberModel = findCost(costs, RUB);
        Assert.assertEquals(1, numberModel.getNumber().longValue());

        costs = mgPaymentRepository.getPaymentsSplitCount("ca2e9162-eda2-4d17-bbfa-dc5e39b1772d", null, 1573554400000L, 1579666000698L, SplitUnit.MONTH);
        numberModel = findCost(costs, RUB);
        Assert.assertEquals(3, numberModel.getNumber().longValue());

        costs = mgPaymentRepository.getPaymentsSplitCount("ca2e9162-eda2-4d17-bbfa-dc5e39b1772d", null, 1573554400000L, 1579666000698L, SplitUnit.YEAR);
        numberModel = findCost(costs, RUB);
        Assert.assertEquals(3, numberModel.getNumber().longValue());
    }

    @NotNull
    private NumberModel findCost(List<? extends NumberModel> costs, String currency) {
        return costs.stream()
                .filter(costEntity -> currency.equals(costEntity.getCurrency()))
                .findFirst()
                .get();
    }

    @Test
    public void testAveragePayment() {
        List<NumberModel> numberModels = mgPaymentRepository.getAveragePayment("ca2e9162-eda2-4d17-bbfa-dc5e39b1772a", null, 1575554400000L, 1575556887697L);
        NumberModel numberModel = findCost(numberModels, RUB);
        Assert.assertEquals(3000L, numberModel.getNumber().longValue());

        numberModels = mgPaymentRepository.getAveragePayment("ca2e9162-eda2-4d17-bbfa-dc5e39b1772a", List.of("ad8b7bfd-0760-4781-a400-51903ee8e502"), 1575554400000L, 1575556887697L);
        numberModel = findCost(numberModels, RUB);
        Assert.assertEquals(5000L, numberModel.getNumber().longValue());

        numberModels = mgPaymentRepository.getAveragePayment("ca2e9162-eda2-4d17-bbfa-dc5e39b173342", null, 1575554400000L, 1575556887697L);
        Assert.assertTrue(numberModels.isEmpty());
    }

    @Test
    public void testCountPayment() {
        List<NumberModel> countModels = mgPaymentRepository.getPaymentsCount("ca2e9162-eda2-4d17-bbfa-dc5e39b1772a", null, 1575554400000L, 1575556887697L);
        NumberModel countModel = findCountModel(countModels, RUB);
        Assert.assertEquals(2, countModel.getNumber().longValue());

        countModels = mgPaymentRepository.getPaymentsCount("ca2e9162-eda2-4d17-bbfa-dc5e39b1772a", List.of("ad8b7bfd-0760-4781-a400-51903ee8e502"), 1575554400000L, 1575556887697L);
        countModel = findCountModel(countModels, RUB);
        Assert.assertEquals(1, countModel.getNumber().longValue());

        countModels = mgPaymentRepository.getPaymentsCount("ca2e9162-eda2-4d17-bbfa-dc5e39b173342", null, 1575554400000L, 1575556887697L);
        Assert.assertTrue(countModels.isEmpty());
    }

    @NotNull
    private NumberModel findCountModel(List<NumberModel> countModels, String currency) {
        return countModels.stream()
                .filter(count -> currency.equals(count.getCurrency()))
                .findFirst()
                .get();
    }

    @Test
    public void testPaymentToolDistr() {
        List<NamingDistribution> toolDistribution = mgPaymentRepository.getPaymentsToolDistribution("ca2e9162-eda2-4d17-bbfa-dc5e39b1772a", null, 1575554400000L, 1575556887697L);

        toolDistribution.forEach(paymentToolDistribution ->
                Assert.assertEquals(50, paymentToolDistribution.getPercent().intValue())
        );

        toolDistribution = mgPaymentRepository.getPaymentsToolDistribution("ca2e9162-eda2-4d17-bbfa-dc5e39b1772a",
                List.of("ad8b7bfd-0760-4781-a400-51903ee8e502"), 1575554400000L, 1575556887697L);

        toolDistribution.forEach(paymentToolDistribution ->
                Assert.assertEquals(100, paymentToolDistribution.getPercent().intValue())
        );
    }

    @Test
    public void testPaymentErrorDistr() {
        List<NamingDistribution> errorDistribution = mgPaymentRepository.getPaymentsErrorDistribution("ca2e9162-eda2-4d17-bbfa-dc5e39b1772k", null, 1575554400000L, 1575599987697L);

        Optional<NamingDistribution> errorResult = errorDistribution.stream()
                .filter(error -> "card is failed".equals(error.getName()))
                .findFirst();
        Assert.assertEquals(errorResult.get().getPercent(), Double.valueOf(33.33));
    }

}
