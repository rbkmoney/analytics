package com.rbkmoney.analytics.converter;

import com.rbkmoney.analytics.dao.model.ShopAmountModel;
import com.rbkmoney.damsel.analytics.ShopAmountResponse;
import com.rbkmoney.damsel.analytics.ShopGroupedAmount;
import org.springframework.stereotype.Component;

import java.util.List;

import static java.util.stream.Collectors.toList;

@Component
public class ShopAmountToResponseConverter {

    public ShopAmountResponse convertShop(List<ShopAmountModel> shopAmountModels) {
        List<ShopGroupedAmount> collect = shopAmountModels.stream()
                .map(cost -> new ShopGroupedAmount()
                        .setAmount(cost.getAmount())
                        .setShopId(cost.getShopId())
                        .setCurrency(cost.getCurrency())
                ).collect(toList());

        return new ShopAmountResponse().setGroupsAmount(collect);
    }

}
