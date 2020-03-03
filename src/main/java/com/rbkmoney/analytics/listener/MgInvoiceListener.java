package com.rbkmoney.analytics.listener;

import com.rbkmoney.analytics.converter.SourceEventParser;
import com.rbkmoney.analytics.flowresolver.FlowResolver;
import com.rbkmoney.analytics.listener.handler.HandlerManager;
import com.rbkmoney.damsel.payment_processing.InvoiceChange;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
@Component
@RequiredArgsConstructor
public class MgInvoiceListener {

    @Value("${kafka.consumer.throttling-timeout-ms}")
    private int throttlingTimeout;

    @Value("${kafka.event-flow.resolver.enabled}")
    private boolean eventFlowResolverEnabled;

    private final SourceEventParser eventParser;
    private final FlowResolver flowResolver;
    private final HandlerManager<InvoiceChange, MachineEvent> handlerManager;

    @KafkaListener(topics = "${kafka.topic.event.sink.initial}", containerFactory = "kafkaListenerContainerFactory")
    public void listen(List<MachineEvent> batch, Acknowledgment ack) throws InterruptedException {
        handleMessages(batch);
        ack.acknowledge();
    }

    private void handleMessages(List<MachineEvent> batch) throws InterruptedException {
        try {
            if (!CollectionUtils.isEmpty(batch)) {
                log.info("MgPaymentAggregatorListener listen batch.size: {}", batch.size());
                batch.stream()
                        .map(machineEvent -> Map.entry(machineEvent, eventParser.parseEvent(machineEvent)))
                        .filter(entry -> entry.getValue().isSetInvoiceChanges())
                        .map(entry -> entry.getValue().getInvoiceChanges().stream()
                                .map(invoiceChange -> Map.entry(entry.getKey(), invoiceChange))
                                .collect(Collectors.toList()))
                        .flatMap(List::stream)
                        .peek(machineEventInvoiceChangeEntry -> {
                            if (eventFlowResolverEnabled)
                                flowResolver.checkFlow(machineEventInvoiceChangeEntry.getValue(), machineEventInvoiceChangeEntry.getKey().getSourceId());
                        })
                        .collect(
                                Collectors.groupingBy(
                                        entry -> Optional.ofNullable(handlerManager.getHandler(entry.getValue())),
                                        Collectors.toList()
                                )
                        )
                        .forEach((handler, entries) -> handler
                                .ifPresent(eventBatchHandler -> eventBatchHandler.handle(entries).execute()));
            }
        } catch (Exception e) {
            log.error("Error when MgPaymentAggregatorListener listen e: ", e);
            Thread.sleep(throttlingTimeout);
            throw e;
        }
    }

}
