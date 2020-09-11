package com.rbkmoney.analytics.dao.utils;

import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.function.Function;

public class QueryUtils {

    public static final String IN = "in";
    public static final String NOT = "not ";
    private static final String EMPTY_STRING = "";

    public static StringBuilder generateInList(List<String> shopIds) {
        return generate(shopIds, IN + " ( ?");
    }

    public static StringBuilder generateNotInList(List<String> shopIds) {
        return generate(shopIds, NOT + IN + " ( ?");
    }

    private static StringBuilder generate(List<String> shopIds, String s) {
        StringBuilder inList = null;
        for (String shopId : shopIds) {
            if (inList == null) {
                inList = new StringBuilder();
                inList.append(s);
            } else {
                inList.append(",?");
            }
        }
        if (inList != null) {
            inList.append(") ");
        }
        return inList;
    }

    public static String generateIdsSql(List<String> shopIds, List<Object> params, Function<List<String>, StringBuilder> func) {
        String excludeShopIdsList;
        if (!CollectionUtils.isEmpty(shopIds)) {
            excludeShopIdsList = " and shopId " + func.apply(shopIds).toString();
            params.addAll(shopIds);
        } else {
            excludeShopIdsList = EMPTY_STRING;
        }
        return excludeShopIdsList;
    }
}
