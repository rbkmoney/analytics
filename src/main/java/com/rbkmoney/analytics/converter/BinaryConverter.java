package com.rbkmoney.analytics.converter;

public interface BinaryConverter<T> {

    T convert(byte[] bin, Class<T> clazz);

}
