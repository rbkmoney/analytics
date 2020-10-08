package com.rbkmoney.analytics.converter;

import com.rbkmoney.analytics.dao.model.NumberModel;
import com.rbkmoney.analytics.dao.model.ShopAmountModel;
import com.rbkmoney.damsel.analytics.AmountResponse;
import com.rbkmoney.damsel.analytics.CurrencyGroupedAmount;
import com.rbkmoney.damsel.analytics.ShopAmountResponse;
import com.rbkmoney.damsel.analytics.ShopGroupedAmount;
import org.springframework.stereotype.Service;

import java.util.List;

import static java.util.stream.Collectors.toList;

@Service
public class CostToAmountResponseConverter {

    public AmountResponse convertCurrency(List<NumberModel> numberModels) {
        List<CurrencyGroupedAmount> collect = numberModels.stream()
                .map(cost -> new CurrencyGroupedAmount()
                        .setAmount(cost.getNumber())
                        .setCurrency(cost.getCurrency())
                ).collect(toList());

        return new AmountResponse()
                .setGroupsAmount(collect);
    }

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
