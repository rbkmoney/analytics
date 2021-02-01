package com.rbkmoney.analytics.listener.handler.merger;

import com.rbkmoney.analytics.dao.repository.postgres.RateDao;
import com.rbkmoney.analytics.domain.db.tables.pojos.Rate;
import com.rbkmoney.analytics.service.model.RateGroupingKey;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@RequiredArgsConstructor
public class RateMerger {

    private final RateDao rateDao;

    public Rate merge(RateGroupingKey key, List<Rate> rates) {
        Rate targetRate = rateDao.getRate(key.getSourceId(), key.getSourceCode(), key.getDestinationCode());
        if (targetRate == null) {
            targetRate = new Rate();
        }
        for (Rate rate : rates) {
            targetRate.setEventId(rate.getEventId());
            targetRate.setEventTime(rate.getEventTime());
            targetRate.setSourceId(key.getSourceId());
            targetRate.setSourceSymbolicCode(key.getSourceCode());
            targetRate.setDestinationSymbolicCode(key.getDestinationCode());

            targetRate.setDestinationExponent(rate.getDestinationExponent() != null ?
                    rate.getDestinationExponent() : targetRate.getDestinationExponent());
            targetRate.setEventTime(rate.getEventTime() != null ? rate.getEventTime() : targetRate.getEventTime());
            targetRate.setExchangeRateRationalP(rate.getExchangeRateRationalP() != null ?
                    rate.getExchangeRateRationalP() : targetRate.getExchangeRateRationalP());
            targetRate.setExchangeRateRationalQ(rate.getExchangeRateRationalQ() != null ?
                    rate.getExchangeRateRationalQ() : targetRate.getExchangeRateRationalQ());
            targetRate.setLowerBoundInclusive(rate.getLowerBoundInclusive() != null ?
                    rate.getLowerBoundInclusive() : targetRate.getLowerBoundInclusive());
            targetRate.setSourceExponent(rate.getSourceExponent() != null ?
                    rate.getSourceExponent() : targetRate.getSourceExponent());
            targetRate.setUpperBoundExclusive(rate.getUpperBoundExclusive() != null ?
                    rate.getUpperBoundExclusive() : targetRate.getUpperBoundExclusive());
        }
        return targetRate;
    }
}
