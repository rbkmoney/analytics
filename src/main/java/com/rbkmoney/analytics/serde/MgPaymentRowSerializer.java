package com.rbkmoney.analytics.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rbkmoney.analytics.dao.model.MgPaymentSinkRow;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

@Slf4j
public class MgPaymentRowSerializer implements Serializer<MgPaymentSinkRow> {

    private final ObjectMapper om = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, MgPaymentSinkRow data) {
        byte[] retVal = null;
        try {
            retVal = om.writeValueAsString(data).getBytes();
        } catch (Exception e) {
            log.error("Error when serialize fraudRequest data: {} ", data, e);
        }
        return retVal;
    }

    @Override
    public void close() {

    }

}