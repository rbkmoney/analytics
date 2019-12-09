package com.rbkmoney.analytics.listener;

import com.rbkmoney.analytics.dao.model.MgPaymentSinkRow;
import com.rbkmoney.analytics.dao.repository.MgPaymentRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@Slf4j
@Component
@RequiredArgsConstructor
public class MgPaymentAggregatorListener {

    private final MgPaymentRepository mgPaymentRepository;

    @KafkaListener(topics = "${kafka.topic.event.sink.aggregated}", containerFactory = "kafkaListenerContainerFactory")
    public void listen(List<MgPaymentSinkRow> batch) {
        log.info("MgPaymentAggregatorListener listen batch.size: {}", batch.size());
        List<MgPaymentSinkRow> resultRaws = batch.stream()
                .flatMap(mgEventSinkRow ->
                        flatMapToList(mgEventSinkRow)
                                .stream())
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        mgPaymentRepository.insertBatch(resultRaws);
    }

    private List<MgPaymentSinkRow> flatMapToList(MgPaymentSinkRow mgPaymentSinkRow) {
        if (mgPaymentSinkRow.getOldMgPaymentSinkRow() == null || mgPaymentSinkRow.getOldMgPaymentSinkRow().getStatus() == null) {
            return List.of(mgPaymentSinkRow);
        }
        return List.of(mgPaymentSinkRow.getOldMgPaymentSinkRow(), mgPaymentSinkRow);
    }

}