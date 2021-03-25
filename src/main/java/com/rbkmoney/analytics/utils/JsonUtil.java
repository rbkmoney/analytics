package com.rbkmoney.analytics.utils;

import com.rbkmoney.geck.serializer.kit.json.JsonHandler;
import com.rbkmoney.geck.serializer.kit.tbase.TBaseProcessor;
import org.apache.thrift.TBase;

import java.io.IOException;

public class JsonUtil {

    public static String thriftBaseToJsonString(TBase thriftBase) {
        try {
            return new TBaseProcessor().process(thriftBase, new JsonHandler()).toString();
        } catch (IOException e) {
            throw new RuntimeException("Couldn't convert to json string: " + thriftBase, e);
        }
    }

}
