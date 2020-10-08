package com.rbkmoney.analytics.converter;

import com.rbkmoney.analytics.dao.model.ShopAmountModel;
import org.springframework.core.convert.converter.Converter;
import org.springframework.stereotype.Component;

import java.math.BigInteger;
import java.util.Map;

@Component
public class RawToShopAmountModelConverter implements Converter<Map, ShopAmountModel> {

    public static final String NUM = "num";
    public static final String SHOP_ID = "shop_id";
    public static final String CURRENCY = "currency";

    @Override
    public ShopAmountModel convert(Map row) {
        ShopAmountModel shopAmountModel = new ShopAmountModel();
        if (row.get(NUM) instanceof Double) {
            shopAmountModel.setAmount((((Double) row.get(NUM)).longValue()));
        } else {
            if (row.get(NUM) instanceof BigInteger) {
                shopAmountModel.setAmount(((BigInteger) row.get(NUM)).longValue());
            } else {
                shopAmountModel.setAmount((long) row.get(NUM));
            }
        }
        shopAmountModel.setShopId((String) row.get(SHOP_ID));
        shopAmountModel.setCurrency((String) row.get(CURRENCY));

        return shopAmountModel;
    }

}
