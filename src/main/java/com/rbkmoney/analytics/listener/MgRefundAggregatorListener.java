package com.rbkmoney.analytics.listener;

import com.rbkmoney.analytics.dao.model.MgRefundRow;
import com.rbkmoney.analytics.dao.repository.MgRefundRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@Slf4j
//@Component
@RequiredArgsConstructor
public class MgRefundAggregatorListener {

    private final MgRefundRepository mgRefundRepository;

//    @KafkaListener(topics = "${kafka.topic.event.sink.aggregatedRefund}", containerFactory = "kafkaListenerRefundContainerFactory")
    public void listen(List<MgRefundRow> batch, Acknowledgment ack) {
        try {
            if (!CollectionUtils.isEmpty(batch)) {
                log.info("MgRefundAggregatorListener listen batch.size: {}", batch.size());
                List<MgRefundRow> resultRaws = batch.stream()
                        .filter(Objects::nonNull)
                        .flatMap(mgEventSinkRow ->
                                flatMapToList(mgEventSinkRow)
                                        .stream())
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());
                mgRefundRepository.insertBatch(resultRaws);
            }
        } catch (Exception e) {
            log.error("Exception when MgPaymentAggregatorListener e: ", e);
            throw e;
        }
        ack.acknowledge();
    }

    private List<MgRefundRow> flatMapToList(MgRefundRow mgPaymentSinkRow) {
        String refundId = mgPaymentSinkRow.getRefundId();
        if (refundId != null && mgPaymentSinkRow.getRefunds() != null && mgPaymentSinkRow.getRefunds().get(refundId) != null) {
            return List.of(mgPaymentSinkRow.getRefunds().get(refundId), mgPaymentSinkRow);
        }
        return List.of(mgPaymentSinkRow);
    }

}
