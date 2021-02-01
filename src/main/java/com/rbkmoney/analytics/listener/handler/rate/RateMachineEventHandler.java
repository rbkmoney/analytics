package com.rbkmoney.analytics.listener.handler.rate;

import com.rbkmoney.analytics.dao.repository.postgres.RateDao;
import com.rbkmoney.analytics.domain.db.tables.pojos.Rate;
import com.rbkmoney.analytics.listener.handler.merger.RateMerger;
import com.rbkmoney.analytics.listener.mapper.ChangeHandler;
import com.rbkmoney.analytics.service.model.RateGroupingKey;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import com.rbkmoney.sink.common.parser.impl.MachineEventParser;
import com.rbkmoney.xrates.rate.Change;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor
public class RateMachineEventHandler {

    @Value("${kafka.consumer.throttling-timeout-ms}")
    private int throttlingTimeout;

    private final MachineEventParser<Change> eventParser;
    private final List<ChangeHandler<Change, MachineEvent, List<Rate>>> changeHandlers;
    private final RateMerger rateMerger;
    private final RateDao rateDao;

    @Transactional(propagation = Propagation.REQUIRED)
    public void handle(List<MachineEvent> batch, Acknowledgment ack) throws InterruptedException {
        try {
            if (CollectionUtils.isEmpty(batch)){
                return;
            }
            for (MachineEvent machineEvent : batch) {
                final Change change = eventParser.parse(machineEvent);
                handleEvent(machineEvent, change);
            }
            ack.acknowledge();
        } catch (Exception e) {
            log.error("Exception during PartyListener process", e);
            Thread.sleep(throttlingTimeout);
            throw e;
        }
    }

    private void handleEvent(MachineEvent machineEvent, Change change) {
        final List<Rate> rates = changeHandlers.stream()
                .filter(changeHandler -> changeHandler.accept(change))
                .flatMap(changeHandler -> changeHandler.handleChange(change, machineEvent).stream())
                .collect(Collectors.groupingBy(o -> new RateGroupingKey(o.getSourceId(),
                        o.getSourceSymbolicCode(), o.getDestinationSymbolicCode()), Collectors.toList()))
                .entrySet().stream()
                .map(rateGroupingKey -> rateMerger.merge(rateGroupingKey.getKey(), rateGroupingKey.getValue()))
                .collect(Collectors.toList());
        if (!rates.isEmpty()) {
            rateDao.saveRateBatch(rates);
        }
    }

}
