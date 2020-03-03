package com.rbkmoney.analytics.flowresolver;

import com.fasterxml.jackson.databind.util.LRUMap;
import com.rbkmoney.damsel.payment_processing.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

@Slf4j
@Component
public class FlowResolver {

    //Предположение, что состояния ивойсов находятся в event-sink достаточно близко друг к другу
    //будут умещаться в размерности 10к инвойсов
    private LRUMap<String, String> statesMap = new LRUMap<>(1000, 10000);

    //Сумма n членов арифметической прогрессии Sn = ( a1 + an ) * n / 2 => (1 + 10) * 10 / 2 = 55 - для флоу из 10 состояний
    //Предположение, что флоу ~ 15 => 15*55 > 1000, в случае если больше будут удаляться на LRUMap
    private PrintableLruMap<String, String> flows = new PrintableLruMap<>(1000, 1000);

    @Value("${kafka.event-flow.resolver.count-between-print:1000000}")
    public int countBetweenPrint;

    private AtomicInteger counter = new AtomicInteger();

    public void checkFlow(InvoiceChange invoiceChange, String invoiceId) {
        if (invoiceChange.isSetInvoicePaymentChange()) {
            int i = counter.incrementAndGet();
            if (i > countBetweenPrint) {
                print();
                counter.set(0);
            }
            InvoicePaymentChange invoicePaymentChange = invoiceChange.getInvoicePaymentChange();
            InvoicePaymentChangePayload payload = invoicePaymentChange.getPayload();

            String paymentUniqId = invoiceId + invoicePaymentChange.getId();

            String flowValue = initNewFlowValue(payload, paymentUniqId);
            String result = flows.get(flowValue);
            if (StringUtils.isEmpty(result)) {
                flows.put(flowValue, flowValue);
            }

            statesMap.put(paymentUniqId, flowValue);
        }

    }

    private String initNewFlowValue(InvoicePaymentChangePayload payload, String paymentUniqId) {
        String key = mapState(payload);
        log.info("statesMap id: {} key: {}", paymentUniqId, key);

        String oldState = statesMap.get(paymentUniqId);
        if (!StringUtils.isEmpty(oldState)) { // check flow is exist
            if (Pattern.compile(key + "$").matcher(oldState).find()) { //check last state is equals current
                return oldState;
            }
            return oldState + "\n-> " + key;
        }
        return key;
    }

    private String mapState(InvoicePaymentChangePayload payload) {
        String key = "";
        if (payload.isSetInvoicePaymentRefundChange()) {
            key += "InvoicePaymentRefundChange:";
            InvoicePaymentRefundChangePayload invoicePaymentRefundChangePayload = payload.getInvoicePaymentRefundChange().getPayload();
            if (invoicePaymentRefundChangePayload.isSetInvoicePaymentRefundCreated()) {
                key += ":InvoicePaymentRefundCreated";
            } else if (invoicePaymentRefundChangePayload.isSetInvoicePaymentRefundStatusChanged()) {
                key += ":InvoicePaymentRefundStatusChanged:" + invoicePaymentRefundChangePayload.getInvoicePaymentRefundStatusChanged().getStatus().getFieldValue();
            }
        } else if (payload.isSetInvoicePaymentStarted()) {
            key += "InvoicePaymentStarted";
        } else if (payload.isSetInvoicePaymentStatusChanged()) {
            key += "InvoicePaymentStatusChanged:" + payload.getInvoicePaymentStatusChanged().getStatus().getFieldValue();
        } else if (payload.isSetInvoicePaymentCaptureStarted()) {
            key += "InvoicePaymentCaptureStarted";
        } else if (payload.isSetInvoicePaymentAdjustmentChange()) {
            key += "InvoicePaymentAdjustmentChange:";
            InvoicePaymentAdjustmentChangePayload invoicePaymentAdjustmentChangePayload = payload.getInvoicePaymentAdjustmentChange().getPayload();
            if (invoicePaymentAdjustmentChangePayload.isSetInvoicePaymentAdjustmentCreated()) {
                key += "InvoicePaymentAdjustmentCreated";
            } else if (invoicePaymentAdjustmentChangePayload.isSetInvoicePaymentAdjustmentStatusChanged()) {
                key += "InvoicePaymentAdjustmentStatusChanged:" + invoicePaymentAdjustmentChangePayload.getInvoicePaymentAdjustmentStatusChanged().getStatus().getFieldValue();
            }
        } else if (payload.isSetInvoicePaymentCashFlowChanged()) {
            key += "InvoicePaymentCashFlowChanged";
        } else if (payload.isSetInvoicePaymentChargebackChange()) {
            key += "InvoicePaymentChargebackChange:";
            InvoicePaymentChargebackChangePayload changePayload = payload.getInvoicePaymentChargebackChange().getPayload();
            if (changePayload.isSetInvoicePaymentChargebackBodyChanged()) {
                key += "InvoicePaymentChargebackBodyChanged";
            } else if (changePayload.isSetInvoicePaymentChargebackCashFlowChanged()) {
                key += "InvoicePaymentChargebackCashFlowChanged";
            } else if (changePayload.isSetInvoicePaymentChargebackCreated()) {
                key += "InvoicePaymentChargebackCreated";
            } else if (changePayload.isSetInvoicePaymentChargebackLevyChanged()) {
                key += "InvoicePaymentChargebackLevyChanged";
            } else if (changePayload.isSetInvoicePaymentChargebackStageChanged()) {
                key += "InvoicePaymentChargebackStageChanged";
            } else if (changePayload.isSetInvoicePaymentChargebackStatusChanged()) {
                key += "InvoicePaymentChargebackStatusChanged:" + changePayload.getInvoicePaymentChargebackStatusChanged().getStatus().getFieldValue();
            } else if (changePayload.isSetInvoicePaymentChargebackTargetStatusChanged()) {
                key += "InvoicePaymentChargebackTargetStatusChanged:" + changePayload.getInvoicePaymentChargebackTargetStatusChanged().getStatus().getFieldValue();
            }
        } else if (payload.isSetInvoicePaymentRecTokenAcquired()) {
            key += "InvoicePaymentRecTokenAcquired";
        } else if (payload.isSetInvoicePaymentRiskScoreChanged()) {
            key += "InvoicePaymentRiskScoreChanged";
        } else if (payload.isSetInvoicePaymentRouteChanged()) {
            key += "InvoicePaymentRouteChanged";
        } else if (payload.isSetInvoicePaymentSessionChange()) {
            key += "InvoicePaymentSessionChange";
        } else {
            key += "unknown";
        }
        return key.replaceAll("\\(.*\\)", "");
    }

    public void print() {
        flows.print();
    }
}
