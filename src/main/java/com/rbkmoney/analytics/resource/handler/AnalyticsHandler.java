package com.rbkmoney.analytics.resource.handler;

import com.rbkmoney.analytics.converter.*;
import com.rbkmoney.analytics.dao.model.NamingDistribution;
import com.rbkmoney.analytics.dao.model.NumberModel;
import com.rbkmoney.analytics.dao.model.SplitNumberModel;
import com.rbkmoney.analytics.dao.model.SplitStatusNumberModel;
import com.rbkmoney.analytics.dao.repository.clickhouse.ClickHousePaymentRepository;
import com.rbkmoney.analytics.dao.repository.clickhouse.ClickHouseRefundRepository;
import com.rbkmoney.damsel.analytics.*;
import com.rbkmoney.geck.common.util.TypeUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class AnalyticsHandler implements AnalyticsServiceSrv.Iface {

    private final ClickHousePaymentRepository clickHousePaymentRepository;
    private final ClickHouseRefundRepository clickHouseRefundRepository;

    private final DaoNamingDistributionsToResponseConverter convertPaymentToolsToResponse;
    private final DaoErrorDistributionsToResponseConverter daoErrorDistributionsToResponse;
    private final CostToAmountResponseConverter costToAmountResponseConverter;
    private final CountModelCountResponseConverter countModelCountResponseConverter;
    private final GroupedCurAmountToResponseConverter groupedCurAmountToResponseConverter;
    private final GroupedCurCountToResponseConverter groupedCurCountToResponseConverter;

    @Override
    public PaymentToolDistributionResponse getPaymentsToolDistribution(FilterRequest filterRequest) {
        log.info("-> getPaymentsToolDistribution filterRequest: {}", filterRequest);

        MerchantFilter merchantFilter = filterRequest.getMerchantFilter();
        TimeFilter timeFilter = filterRequest.getTimeFilter();

        List<NamingDistribution> paymentsToolDistribution = clickHousePaymentRepository.getPaymentsToolDistribution(
                merchantFilter.getPartyId(),
                merchantFilter.getShopIds(),
                TypeUtil.stringToLocalDateTime(timeFilter.getFromTime()),
                TypeUtil.stringToLocalDateTime(timeFilter.getToTime())
        );

        PaymentToolDistributionResponse paymentToolDistributionResponse = convertPaymentToolsToResponse.convert(paymentsToolDistribution);
        log.info("<- getPaymentsToolDistribution paymentToolDistributionResponse: {}", paymentToolDistributionResponse);
        return paymentToolDistributionResponse;
    }

    private Long convertToMillis(String fromTime) {
        LocalDateTime localDateTime = TypeUtil.stringToLocalDateTime(fromTime);
        return localDateTime.toInstant(ZoneOffset.UTC).toEpochMilli();
    }

    @Override
    public AmountResponse getPaymentsAmount(FilterRequest filterRequest) {
        log.info("-> getPaymentsAmount filterRequest: {}", filterRequest);
        MerchantFilter merchantFilter = filterRequest.getMerchantFilter();
        TimeFilter timeFilter = filterRequest.getTimeFilter();

        List<NumberModel> paymentsToolDistribution = clickHousePaymentRepository.getPaymentsAmount(
                merchantFilter.getPartyId(),
                merchantFilter.getShopIds(),
                TypeUtil.stringToLocalDateTime(timeFilter.getFromTime()),
                TypeUtil.stringToLocalDateTime(timeFilter.getToTime())
        );

        AmountResponse amountResponse = costToAmountResponseConverter.convert(paymentsToolDistribution);
        log.info("<- getPaymentsAmount amountResponse: {}", amountResponse);
        return amountResponse;
    }

    @Override
    public AmountResponse getAveragePayment(FilterRequest filterRequest) {
        log.info("-> getAveragePayment filterRequest: {}", filterRequest);

        MerchantFilter merchantFilter = filterRequest.getMerchantFilter();
        TimeFilter timeFilter = filterRequest.getTimeFilter();

        List<NumberModel> paymentsToolDistribution = clickHousePaymentRepository.getAveragePayment(
                merchantFilter.getPartyId(),
                merchantFilter.getShopIds(),
                TypeUtil.stringToLocalDateTime(timeFilter.getFromTime()),
                TypeUtil.stringToLocalDateTime(timeFilter.getToTime())
        );

        AmountResponse amountResponse = costToAmountResponseConverter.convert(paymentsToolDistribution);
        log.info("<- getAveragePayment amountResponse: {}", amountResponse);
        return amountResponse;
    }

    @Override
    public CountResponse getPaymentsCount(FilterRequest filterRequest) {
        log.info("-> getPaymentsCount filterRequest: {}", filterRequest);

        MerchantFilter merchantFilter = filterRequest.getMerchantFilter();
        TimeFilter timeFilter = filterRequest.getTimeFilter();

        List<NumberModel> paymentsToolDistribution = clickHousePaymentRepository.getPaymentsCount(
                merchantFilter.getPartyId(),
                merchantFilter.getShopIds(),
                TypeUtil.stringToLocalDateTime(timeFilter.getFromTime()),
                TypeUtil.stringToLocalDateTime(timeFilter.getToTime())
        );

        CountResponse countResponse = countModelCountResponseConverter.convert(paymentsToolDistribution);
        log.info("<- getPaymentsCount countResponse: {}", countResponse);
        return countResponse;
    }

    @Override
    public ErrorDistributionsResponse getPaymentsErrorDistribution(FilterRequest filterRequest) {
        log.info("-> getPaymentsErrorDistribution filterRequest: {}", filterRequest);

        MerchantFilter merchantFilter = filterRequest.getMerchantFilter();
        TimeFilter timeFilter = filterRequest.getTimeFilter();

        List<NamingDistribution> namingDistributions = clickHousePaymentRepository.getPaymentsErrorDistribution(
                merchantFilter.getPartyId(),
                merchantFilter.getShopIds(),
                TypeUtil.stringToLocalDateTime(timeFilter.getFromTime()),
                TypeUtil.stringToLocalDateTime(timeFilter.getToTime())
        );

        ErrorDistributionsResponse errorDistributionsResponse = daoErrorDistributionsToResponse.convert(namingDistributions);
        log.info("<- getPaymentsErrorDistribution errorDistributionsResponse: {}", errorDistributionsResponse);
        return errorDistributionsResponse;
    }

    @Override
    public SplitAmountResponse getPaymentsSplitAmount(SplitFilterRequest splitFilterRequest) {
        log.info("-> getPaymentsSplitAmount splitFilterRequest: {}", splitFilterRequest);

        FilterRequest filterRequest = splitFilterRequest.getFilterRequest();
        SplitUnit splitUnit = splitFilterRequest.getSplitUnit();
        MerchantFilter merchantFilter = filterRequest.getMerchantFilter();
        TimeFilter timeFilter = filterRequest.getTimeFilter();

        List<SplitNumberModel> splitAmount = clickHousePaymentRepository.getPaymentsSplitAmount(
                merchantFilter.getPartyId(),
                merchantFilter.getShopIds(),
                TypeUtil.stringToLocalDateTime(timeFilter.getFromTime()),
                TypeUtil.stringToLocalDateTime(timeFilter.getToTime()),
                splitUnit
        );

        SplitAmountResponse splitAmountResponse = groupedCurAmountToResponseConverter.convert(splitAmount);
        splitAmountResponse.setResultSplitUnit(splitUnit);

        log.info("<- getPaymentsSplitCount splitAmountResponse: {}", splitAmountResponse);
        return splitAmountResponse;
    }


    @Override
    public SplitCountResponse getPaymentsSplitCount(SplitFilterRequest splitFilterRequest) {
        log.info("-> getPaymentsSplitCount splitFilterRequest: {}", splitFilterRequest);

        FilterRequest filterRequest = splitFilterRequest.getFilterRequest();
        SplitUnit splitUnit = splitFilterRequest.getSplitUnit();
        MerchantFilter merchantFilter = filterRequest.getMerchantFilter();
        TimeFilter timeFilter = filterRequest.getTimeFilter();

        List<SplitStatusNumberModel> splitAmount = clickHousePaymentRepository.getPaymentsSplitCount(
                merchantFilter.getPartyId(),
                merchantFilter.getShopIds(),
                TypeUtil.stringToLocalDateTime(timeFilter.getFromTime()),
                TypeUtil.stringToLocalDateTime(timeFilter.getToTime()),
                splitUnit
        );

        SplitCountResponse splitCountResponse = groupedCurCountToResponseConverter.convert(splitAmount);
        splitCountResponse.setResultSplitUnit(splitUnit);

        log.info("<- getPaymentsSplitCount splitCountResponse: {}", splitCountResponse);
        return splitCountResponse;
    }

    @Override
    public AmountResponse getRefundsAmount(FilterRequest filterRequest) {
        log.info("-> getRefundsAmount filterRequest: {}", filterRequest);

        MerchantFilter merchantFilter = filterRequest.getMerchantFilter();
        TimeFilter timeFilter = filterRequest.getTimeFilter();

        List<NumberModel> paymentsToolDistribution = clickHouseRefundRepository.getPaymentsAmount(
                merchantFilter.getPartyId(),
                merchantFilter.getShopIds(),
                TypeUtil.stringToLocalDateTime(timeFilter.getFromTime()),
                TypeUtil.stringToLocalDateTime(timeFilter.getToTime())
        );

        AmountResponse amountResponse = costToAmountResponseConverter.convert(paymentsToolDistribution);

        log.info("<- getRefundsAmount amountResponse: {}", amountResponse);
        return amountResponse;
    }
}
