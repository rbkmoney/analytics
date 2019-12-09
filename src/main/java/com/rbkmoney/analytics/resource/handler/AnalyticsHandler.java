package com.rbkmoney.analytics.resource.handler;

import com.rbkmoney.analytics.converter.*;
import com.rbkmoney.analytics.dao.model.NamingDistribution;
import com.rbkmoney.analytics.dao.model.NumberModel;
import com.rbkmoney.analytics.dao.model.SplitNumberModel;
import com.rbkmoney.analytics.dao.model.SplitStatusNumberModel;
import com.rbkmoney.analytics.dao.repository.MgPaymentRepository;
import com.rbkmoney.analytics.dao.repository.MgRefundRepository;
import com.rbkmoney.damsel.analytics.*;
import com.rbkmoney.geck.common.util.TypeUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@RequiredArgsConstructor
public class AnalyticsHandler implements AnalyticsServiceSrv.Iface {

    private final MgPaymentRepository mgPaymentRepository;
    private final MgRefundRepository mgRefundRepository;

    private final DaoNamingDistributionsToResponse convertPaymentToolsToResponse;
    private final DaoErrorDistributionsToResponse daoErrorDistributionsToResponse;
    private final CostToAmountResponse costToAmountResponseConverter;
    private final CountModelCountResponseConverter countModelCountResponseConverter;
    private final GroupedCurAmountToResponseConverter groupedCurAmountToResponseConverter;

    @Override
    public PaymentToolDistributionResponse getPaymentsToolDistribution(FilterRequest filterRequest) {
        MerchantFilter merchantFilter = filterRequest.getMerchantFilter();
        TimeFilter timeFilter = filterRequest.getTimeFilter();

        List<NamingDistribution> paymentsToolDistribution = mgPaymentRepository.getPaymentsToolDistribution(
                merchantFilter.getPartyId(),
                merchantFilter.getShopIds(),
                convertToMillis(timeFilter.getFromTime()),
                convertToMillis(timeFilter.getToTime())
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

        List<NumberModel> paymentsToolDistribution = mgPaymentRepository.getPaymentsAmount(
                merchantFilter.getPartyId(),
                merchantFilter.getShopIds(),
                convertToMillis(timeFilter.getFromTime()),
                convertToMillis(timeFilter.getToTime())
        );

        return costToAmountResponseConverter.convert(paymentsToolDistribution);
    }

    @Override
    public AmountResponse getAveragePayment(FilterRequest filterRequest) {
        MerchantFilter merchantFilter = filterRequest.getMerchantFilter();
        TimeFilter timeFilter = filterRequest.getTimeFilter();

        List<NumberModel> paymentsToolDistribution = mgPaymentRepository.getAveragePayment(
                merchantFilter.getPartyId(),
                merchantFilter.getShopIds(),
                convertToMillis(timeFilter.getFromTime()),
                convertToMillis(timeFilter.getToTime())
        );

        return costToAmountResponseConverter.convert(paymentsToolDistribution);
    }

    @Override
    public CountResponse getPaymentsCount(FilterRequest filterRequest) {
        MerchantFilter merchantFilter = filterRequest.getMerchantFilter();
        TimeFilter timeFilter = filterRequest.getTimeFilter();

        List<NumberModel> paymentsToolDistribution = mgPaymentRepository.getPaymentsCount(
                merchantFilter.getPartyId(),
                merchantFilter.getShopIds(),
                convertToMillis(timeFilter.getFromTime()),
                convertToMillis(timeFilter.getToTime())
        );

        return countModelCountResponseConverter.convert(paymentsToolDistribution);
    }

    @Override
    public ErrorDistributionsResponse getPaymentsErrorDistribution(FilterRequest filterRequest) {
        MerchantFilter merchantFilter = filterRequest.getMerchantFilter();
        TimeFilter timeFilter = filterRequest.getTimeFilter();

        List<NamingDistribution> namingDistributions = mgPaymentRepository.getPaymentsErrorDistribution(
                merchantFilter.getPartyId(),
                merchantFilter.getShopIds(),
                convertToMillis(timeFilter.getFromTime()),
                convertToMillis(timeFilter.getToTime())
        );

        return daoErrorDistributionsToResponse.convert(namingDistributions);
    }

    @Override
    public SplitAmountResponse getPaymentsSplitAmount(SplitFilterRequest splitFilterRequest) {
        FilterRequest filterRequest = splitFilterRequest.getFilterRequest();
        SplitUnit splitUnit = splitFilterRequest.getSplitUnit();
        MerchantFilter merchantFilter = filterRequest.getMerchantFilter();
        TimeFilter timeFilter = filterRequest.getTimeFilter();

        List<SplitNumberModel> splitAmount = mgPaymentRepository.getPaymentsSplitAmount(
                merchantFilter.getPartyId(),
                merchantFilter.getShopIds(),
                convertToMillis(timeFilter.getFromTime()),
                convertToMillis(timeFilter.getToTime()),
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

        List<SplitStatusNumberModel> splitAmount = mgPaymentRepository.getPaymentsSplitCount(
                merchantFilter.getPartyId(),
                merchantFilter.getShopIds(),
                convertToMillis(timeFilter.getFromTime()),
                convertToMillis(timeFilter.getToTime()),
                splitUnit
        );

//        return groupedCurAmountToResponseConverter.convert(splitAmount);

        return null;
    }

    @Override
    public AmountResponse getRefundsAmount(FilterRequest filterRequest) {
        MerchantFilter merchantFilter = filterRequest.getMerchantFilter();
        TimeFilter timeFilter = filterRequest.getTimeFilter();

        List<NumberModel> paymentsToolDistribution = mgRefundRepository.getPaymentsAmount(
                merchantFilter.getPartyId(),
                merchantFilter.getShopIds(),
                convertToMillis(timeFilter.getFromTime()),
                convertToMillis(timeFilter.getToTime())
        );

        return costToAmountResponseConverter.convert(paymentsToolDistribution);
    }
}