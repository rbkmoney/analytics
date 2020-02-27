package com.rbkmoney.analytics.converter;

import com.rbkmoney.analytics.dao.model.NumberModel;
import org.springframework.core.convert.converter.Converter;
import org.springframework.stereotype.Component;

import java.math.BigInteger;
import java.util.Map;

@Component
public class RawToNumModelConverter implements Converter<Map, NumberModel> {

    public static final String NUM = "num";
    public static final String CURRENCY = "currency";

    @Override
    public NumberModel convert(Map row) {
        NumberModel numberModel = new NumberModel();
        if (row.get(NUM) instanceof Double) {
            numberModel.setNumber((((Double) row.get(NUM)).longValue()));
        } else {
            if (row.get(NUM) instanceof BigInteger) {
                numberModel.setNumber(((BigInteger) row.get(NUM)).longValue());
            } else {
                numberModel.setNumber((long) row.get(NUM));

            }
        }
        numberModel.setCurrency((String) row.get(CURRENCY));
        return numberModel;
    }
}
