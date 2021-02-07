package com.rbkmoney.analytics.listener.mapper.rate;

import com.rbkmoney.analytics.constant.EventType;
import com.rbkmoney.analytics.domain.db.tables.pojos.Rate;
import com.rbkmoney.analytics.listener.mapper.Mapper;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import com.rbkmoney.xrates.base.Rational;
import com.rbkmoney.xrates.base.TimestampInterval;
import com.rbkmoney.xrates.rate.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Component
@RequiredArgsConstructor
public class RateCreatedMapper implements Mapper<Change, MachineEvent, List<Rate>> {

    @Override
    public List<Rate> map(Change change, MachineEvent event) {
        if (change.getCreated().getExchangeRateData().getQuotes().isEmpty()) {
            log.warn("Quotes is empty, SinkEvent will not be saved, eventId={}, sourceId={}",
                    event.getEventId(), event.getSourceId());
            return List.of();
        }
        log.info("Start rate created handling, eventId={}, sourceId={}", event.getEventId(), event.getSourceId());

        ExchangeRateCreated exchangeRateCreated = change.getCreated();
        ExchangeRateData exchangeRateData = exchangeRateCreated.getExchangeRateData();
        TimestampInterval interval = exchangeRateData.getInterval();

        return exchangeRateData.getQuotes().stream()
                .map(quote -> initRate(event, interval, quote))
                .collect(Collectors.toList());
    }

    @NotNull
    private Rate initRate(MachineEvent event, TimestampInterval interval, Quote quote) {
        Rate rate = new Rate();
        rate.setSourceId(event.getSourceId());
        rate.setEventId(event.getEventId());
        rate.setEventTime(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));

        // Quote
        Currency source = quote.getSource();
        Currency destination = quote.getDestination();
        Rational exchangeRate = quote.getExchangeRate();

        // Currency
        rate.setSourceSymbolicCode(source.getSymbolicCode());
        rate.setSourceExponent(source.getExponent());
        rate.setDestinationSymbolicCode(destination.getSymbolicCode());
        rate.setDestinationExponent(destination.getExponent());

        // ExchangeRate
        rate.setExchangeRateRationalP(exchangeRate.getP());
        rate.setExchangeRateRationalQ(exchangeRate.getQ());

        rate.setLowerBoundInclusive(TypeUtil.stringToLocalDateTime(interval.getLowerBoundInclusive()));
        rate.setUpperBoundExclusive(TypeUtil.stringToLocalDateTime(interval.getUpperBoundExclusive()));

        return rate;
    }

    @Override
    public EventType getChangeType() {
        return EventType.RATE_CREATED;
    }
}
