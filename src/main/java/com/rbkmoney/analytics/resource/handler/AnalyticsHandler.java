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
        MerchantFilter merchantFilter = filterRequest.getMerchantFilter();
        TimeFilter timeFilter = filterRequest.getTimeFilter();

        List<NamingDistribution> paymentsToolDistribution = clickHousePaymentRepository.getPaymentsToolDistribution(
                merchantFilter.getPartyId(),
                merchantFilter.getShopIds(),
                TypeUtil.stringToLocalDateTime(timeFilter.getFromTime()),
                TypeUtil.stringToLocalDateTime(timeFilter.getToTime())
        );

        return convertPaymentToolsToResponse.convert(paymentsToolDistribution);
    }

    private Long convertToMillis(String fromTime) {
        LocalDateTime localDateTime = TypeUtil.stringToLocalDateTime(fromTime);
        return localDateTime.toInstant(ZoneOffset.UTC).toEpochMilli();
    }

    @Override
    public AmountResponse getPaymentsAmount(FilterRequest filterRequest) {
        MerchantFilter merchantFilter = filterRequest.getMerchantFilter();
        TimeFilter timeFilter = filterRequest.getTimeFilter();

        List<NumberModel> paymentsToolDistribution = clickHousePaymentRepository.getPaymentsAmount(
                merchantFilter.getPartyId(),
                merchantFilter.getShopIds(),
                TypeUtil.stringToLocalDateTime(timeFilter.getFromTime()),
                TypeUtil.stringToLocalDateTime(timeFilter.getToTime())
        );

        return costToAmountResponseConverter.convert(paymentsToolDistribution);
    }

    @Override
    public AmountResponse getAveragePayment(FilterRequest filterRequest) {
        MerchantFilter merchantFilter = filterRequest.getMerchantFilter();
        TimeFilter timeFilter = filterRequest.getTimeFilter();

        List<NumberModel> paymentsToolDistribution = clickHousePaymentRepository.getAveragePayment(
                merchantFilter.getPartyId(),
                merchantFilter.getShopIds(),
                TypeUtil.stringToLocalDateTime(timeFilter.getFromTime()),
                TypeUtil.stringToLocalDateTime(timeFilter.getToTime())
        );

        return costToAmountResponseConverter.convert(paymentsToolDistribution);
    }

    @Override
    public CountResponse getPaymentsCount(FilterRequest filterRequest) {
        MerchantFilter merchantFilter = filterRequest.getMerchantFilter();
        TimeFilter timeFilter = filterRequest.getTimeFilter();

        List<NumberModel> paymentsToolDistribution = clickHousePaymentRepository.getPaymentsCount(
                merchantFilter.getPartyId(),
                merchantFilter.getShopIds(),
                TypeUtil.stringToLocalDateTime(timeFilter.getFromTime()),
                TypeUtil.stringToLocalDateTime(timeFilter.getToTime())
        );

        return countModelCountResponseConverter.convert(paymentsToolDistribution);
    }

    @Override
    public ErrorDistributionsResponse getPaymentsErrorDistribution(FilterRequest filterRequest) {
        MerchantFilter merchantFilter = filterRequest.getMerchantFilter();
        TimeFilter timeFilter = filterRequest.getTimeFilter();

        List<NamingDistribution> namingDistributions = clickHousePaymentRepository.getPaymentsErrorDistribution(
                merchantFilter.getPartyId(),
                merchantFilter.getShopIds(),
                TypeUtil.stringToLocalDateTime(timeFilter.getFromTime()),
                TypeUtil.stringToLocalDateTime(timeFilter.getToTime())
        );

        return daoErrorDistributionsToResponse.convert(namingDistributions);
    }

    @Override
    public SplitAmountResponse getPaymentsSplitAmount(SplitFilterRequest splitFilterRequest) {
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

        return groupedCurAmountToResponseConverter.convert(splitAmount);
    }


    @Override
    public SplitCountResponse getPaymentsSplitCount(SplitFilterRequest splitFilterRequest) {
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

        return groupedCurCountToResponseConverter.convert(splitAmount);
    }

    @Override
    public AmountResponse getRefundsAmount(FilterRequest filterRequest) {
        MerchantFilter merchantFilter = filterRequest.getMerchantFilter();
        TimeFilter timeFilter = filterRequest.getTimeFilter();

        List<NumberModel> paymentsToolDistribution = clickHouseRefundRepository.getPaymentsAmount(
                merchantFilter.getPartyId(),
                merchantFilter.getShopIds(),
                TypeUtil.stringToLocalDateTime(timeFilter.getFromTime()),
                TypeUtil.stringToLocalDateTime(timeFilter.getToTime())
        );

        return costToAmountResponseConverter.convert(paymentsToolDistribution);
    }
}
