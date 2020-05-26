package com.rbkmoney.analytics.utils;

import com.rbkmoney.damsel.analytics.SubError;
import org.springframework.util.StringUtils;

public class SubErrorGenerator {

    public static final String EMPTY_ERROR_CODE = "empty_error_code";

    public static SubError generateError(String name) {
        if (!StringUtils.isEmpty(name)) {
            String[] split = name.split(":");
            int i = 0;
            return createSubError(split, i);
        }
        return new SubError(EMPTY_ERROR_CODE);
    }

    private static SubError createSubError(String[] names, int next) {
        if (names.length > next) {
            return new SubError(names[next])
                    .setSubError(createSubError(names, ++next));
        }
        return null;
    }
}
