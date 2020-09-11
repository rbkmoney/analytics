package com.rbkmoney.analytics.dao.repository.clickhouse.iface;

import com.rbkmoney.analytics.dao.model.*;
import com.rbkmoney.damsel.analytics.SplitUnit;

import java.time.LocalDateTime;
import java.util.List;

public interface ClickHousePaymentRepository {

    void insertBatch(List<PaymentRow> paymentRows);

    List<NumberModel> getAveragePayment(String partyId, List<String> shopIds, List<String> excludeShopIds, LocalDateTime from, LocalDateTime to);

    List<NumberModel> getPaymentsAmount(String partyId, List<String> shopIds, List<String> excludeShopIds, LocalDateTime from, LocalDateTime to);

    List<NumberModel> getPaymentsCount(String partyId, List<String> shopIds, List<String> excludeShopIds, LocalDateTime from, LocalDateTime to);

    List<SplitNumberModel> getPaymentsSplitAmount(String partyId, List<String> shopIds, List<String> excludeShopIds, LocalDateTime from, LocalDateTime to, SplitUnit splitUnit);

    List<SplitStatusNumberModel> getPaymentsSplitCount(String partyId, List<String> shopIds, List<String> excludeShopIds, LocalDateTime from, LocalDateTime to, SplitUnit splitUnit);

    List<NamingDistribution> getPaymentsToolDistribution(String partyId, List<String> shopIds, List<String> excludeShopIds, LocalDateTime from, LocalDateTime to);

    List<NamingDistribution> getPaymentsErrorReasonDistribution(String partyId, List<String> shopIds, List<String> excludeShopIds, LocalDateTime from, LocalDateTime to);

    List<NamingDistribution> getPaymentsErrorCodeDistribution(String partyId, List<String> shopIds, List<String> excludeShopIds, LocalDateTime from, LocalDateTime to);

    List<NumberModel> getCurrentBalances(String partyId, List<String> shopIds, List<String> excludeShopIds);

}
