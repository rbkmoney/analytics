package com.rbkmoney.analytics.utils;

import io.micrometer.core.instrument.util.IOUtils;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class FileUtil {

    public static String getFile(String fileName) {
        ClassLoader classLoader = FileUtil.class.getClassLoader();
        return IOUtils.toString(classLoader.getResourceAsStream(fileName), StandardCharsets.UTF_8);
    }

}
