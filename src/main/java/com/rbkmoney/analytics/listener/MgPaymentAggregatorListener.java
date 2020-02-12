package com.rbkmoney.analytics.listener;

import com.rbkmoney.analytics.constant.PaymentStatus;
import com.rbkmoney.analytics.constant.PaymentToolType;
import com.rbkmoney.damsel.domain.*;
import com.rbkmoney.damsel.payment_processing.InvoiceChange;
import com.rbkmoney.damsel.payment_processing.InvoicePaymentChange;
import com.rbkmoney.damsel.payment_processing.InvoicePaymentChangePayload;
import com.rbkmoney.damsel.payment_processing.InvoicePaymentStarted;
import com.rbkmoney.geck.common.util.TBaseUtil;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Component
@RequiredArgsConstructor
public class MgPaymentAggregatorListener {

    public static final String MOBILE = "mobile";
    public static final String PAYMENT_TERMINAL = "payment_terminal";
    public static final String CRYPTO_CUR = "crypto_cur";
    public static final String DIGITAL_WALLET = "digital_wallet";
    private final SourceEventParser eventParser;
    private final ThreadLocal<HashMap<String, StatModel>> sumByCardType = ThreadLocal.withInitial(HashMap::new);
    private final ThreadLocal<HashMap<String, StatModel>> countStatuses = ThreadLocal.withInitial(HashMap::new);

    private AtomicInteger count = new AtomicInteger();
    private AtomicInteger currentYear = new AtomicInteger();

    @Value("${log.count:10000000}")
    private int logCount;

    @KafkaListener(topics = "${kafka.topic.event.sink.initial}", containerFactory = "kafkaListenerContainerFactory")
    public void listen(@Payload List<MachineEvent> messages, @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition, Acknowledgment ack) {
        try {
            handle(messages, partition);
        } catch (Exception e) {
            log.warn("Can't aggr some message: {}", messages);
        }
        ack.acknowledge();
    }

    public void handle(List<MachineEvent> machineEvents, int partition) {
        machineEvents.stream()
                .map(machineEvent -> Map.entry(machineEvent, eventParser.parseEvent(machineEvent)))
                .filter(entry -> entry.getValue().isSetInvoiceChanges())
                .map(entry -> {
                            List<Map.Entry<MachineEvent, InvoiceChange>> invoiceChangesWithMachineEvent = new ArrayList<>();
                            for (InvoiceChange invoiceChange : entry.getValue().getInvoiceChanges()) {
                                invoiceChangesWithMachineEvent.add(Map.entry(entry.getKey(), invoiceChange));
                            }
                            return invoiceChangesWithMachineEvent;
                        }
                )
                .flatMap(List::stream)
                .forEach(machineEventInvoiceChangeEntry -> handleEvent(machineEventInvoiceChangeEntry, partition))
        ;
    }

    private void handleEvent(Map.Entry<MachineEvent, InvoiceChange> machineEventInvoiceChangeEntry, int partition) {
        InvoiceChange value = machineEventInvoiceChangeEntry.getValue();

        if (value.isSetInvoicePaymentChange()) {
            InvoicePaymentChange invoicePaymentChange = value.getInvoicePaymentChange();
            InvoicePaymentChangePayload payload = invoicePaymentChange.getPayload();
            MachineEvent key = machineEventInvoiceChangeEntry.getKey();
            LocalDateTime localDateTime = TypeUtil.stringToLocalDateTime(key.getCreatedAt());
            int year = localDateTime.getYear();

            if (payload.isSetInvoicePaymentStarted()) {


                InvoicePaymentStarted invoicePaymentStarted = payload.getInvoicePaymentStarted();
                InvoicePayment payment = invoicePaymentStarted.getPayment();
                long amount = payment.getCost().getAmount();
                String currency = payment.getCost().getCurrency().getSymbolicCode();

                Payer payer = payment.getPayer();
                PaymentTool paymentTool = null;

                if (payer.isSetPaymentResource()) {
                    DisposablePaymentResource resource = payer.getPaymentResource().getResource();
                    paymentTool = resource.getPaymentTool();
                } else if (payer.isSetCustomer()) {
                    CustomerPayer customer = payer.getCustomer();
                    paymentTool = customer.getPaymentTool();
                } else if (payer.isSetRecurrent()) {
                    RecurrentPayer recurrent = payer.getRecurrent();
                    paymentTool = recurrent.getPaymentTool();
                }

                if (paymentTool != null) {
                    PaymentToolType paymentToolType = TBaseUtil.unionFieldToEnum(paymentTool, PaymentToolType.class);
                    BankCardPaymentSystem paymentSystem = null;
                    if (paymentTool.isSetBankCard()) {
                        BankCard bankCard = paymentTool.getBankCard();
                        calculateForKey(year, payment, amount, bankCard.getPaymentSystem().name(), currency);
                    } else if (paymentTool.isSetMobileCommerce()) {
                        calculateForKey(year, payment, amount, MOBILE, currency);
                    } else if (paymentTool.isSetPaymentTerminal()) {
                        calculateForKey(year, payment, amount, PAYMENT_TERMINAL, currency);
                    } else if (paymentTool.isSetCryptoCurrency()) {
                        calculateForKey(year, payment, amount, CRYPTO_CUR, currency);
                    } else if (paymentTool.isSetDigitalWallet()) {
                        calculateForKey(year, payment, amount, DIGITAL_WALLET, currency);
                    }
                }

            } else if (payload.isSetInvoicePaymentStatusChanged()) {
                InvoicePaymentStatus status = payload.getInvoicePaymentStatusChanged().getStatus();
                PaymentStatus paymentStatus = TBaseUtil.unionFieldToEnum(status, PaymentStatus.class);
                StatModel statModel = countStatuses.get().get(generateKeyPSystem(year, paymentStatus.name()));
                statModel = calculateBankCardData(0L, statModel);
                countStatuses.get().put(generateKeyPSystem(year, paymentStatus.name()), statModel);
            }

            int i = count.incrementAndGet();

            if (i > logCount || currentYear.get() != year) {
                count.set(0);
                if (currentYear.get() != year) {
                    log.info("It's a new year {} -> {} partition: {} sumByCardType: {} countStatuses: {}",
                            currentYear.get(), year, partition, sumByCardType.get(), countStatuses.get());
                }
                log.info("partition: {} sumByCardType: {}", partition, sumByCardType.get());
                log.info("partition: {} countStatuses: {}", partition, countStatuses.get());
            }

            currentYear.set(year);
        }
    }

    private void calculateForKey(int year, InvoicePayment payment, long amount, String name, String currency) {
        StatModel statModel = sumByCardType.get().get(generateKeyPSystem(year, name + "_" + currency));

        InvoicePaymentStatus status = payment.getStatus();
        statModel = calculateBankCardData(amount, statModel);

        sumByCardType.get().put(generateKeyPSystem(year, name + "_" + currency), statModel);
    }

    private String generateKeyPSystem(int year, String prefix) {
        return prefix + "_" + year;
    }

    private StatModel calculateBankCardData(long amount, StatModel statModel) {

        if (statModel == null) {
            statModel = new StatModel();
            statModel.setSum(0L);
            statModel.setCount(0L);
        }

        statModel.setCount(statModel.getCount() + 1);
        statModel.setSum(statModel.getSum() + amount);

        return statModel;
    }

}
