package com.rbkmoney.analytics.listener;

import com.rbkmoney.analytics.converter.SourceEventParser;
import com.rbkmoney.analytics.dao.model.MgPaymentSinkRow;
import com.rbkmoney.analytics.dao.repository.MgPaymentRepository;
import com.rbkmoney.damsel.payment_processing.*;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

@Slf4j
@Component
@RequiredArgsConstructor
public class MgInvoiceListener {

    @Value("${kafka.consumer.throttling-timeout-ms}")
    private int throttleTimeout;

    private final UserInfo userInfo = new UserInfo("analytics", UserType.service_user(new ServiceUser()));

    private final MgPaymentRepository mgPaymentRepository;
    private final InvoicingSrv.Iface invoicingClient;
    private final SourceEventParser eventParser;
    private final InvoicePaymentStatusChangedHandlerImpl handler;

    @KafkaListener(topics = "${kafka.topics.invoicing}", containerFactory = "kafkaListenerContainerFactory")
    public void listen(List<MachineEvent> batch, Acknowledgment ack) throws InterruptedException {
        try {
            List<MgPaymentSinkRow> resultRaws = new ArrayList<>();
            if (!CollectionUtils.isEmpty(batch)) {
                log.info("MgPaymentAggregatorListener listen batch.size: {}", batch.size());
                resultRaws = batch.stream()
                        .filter(Objects::nonNull)
                        .map(machineEvent -> Map.entry(machineEvent, eventParser.parseEvent(machineEvent)))
                        .filter(entry -> entry.getValue().isSetInvoiceChanges())
                        .map(entry -> entry.getValue().getInvoiceChanges().stream()
                                .filter(this::filterFinishSteps)
                                .map(invoiceChange -> Map.entry(entry.getKey(), invoiceChange))
                                .collect(Collectors.toList()))
                        .flatMap(List::stream)
                        .map(entry -> handler.handle(entry.getValue(), entry.getKey()))
                        .collect(Collectors.toList());
                if (!CollectionUtils.isEmpty(resultRaws)) {
                    log.info("MgPaymentAggregatorListener listen batch.size: {} resultRawsFirst: {}", resultRaws.size(),
                            resultRaws.get(0).getInvoiceId());
                }
                mgPaymentRepository.insertBatch(resultRaws);
            }
        } catch (Exception e) {
            log.error("Error when MgPaymentAggregatorListener listen e: ", e);
            Thread.sleep(throttleTimeout);
            throw e;
        }
        ack.acknowledge();
    }

    private boolean filterFinishSteps(InvoiceChange invoiceChange) {
        return invoiceChange.isSetInvoicePaymentChange()
                && invoiceChange.getInvoicePaymentChange().getPayload().isSetInvoicePaymentStatusChanged()
                && (invoiceChange.getInvoicePaymentChange().getPayload().getInvoicePaymentStatusChanged().getStatus().isSetCaptured()
                || invoiceChange.getInvoicePaymentChange().getPayload().getInvoicePaymentStatusChanged().getStatus().isSetCancelled()
                || invoiceChange.getInvoicePaymentChange().getPayload().getInvoicePaymentStatusChanged().getStatus().isSetFailed()
                || invoiceChange.getInvoicePaymentChange().getPayload().getInvoicePaymentStatusChanged().getStatus().isSetRefunded());
    }

}
