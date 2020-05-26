package com.rbkmoney.analytics.utils;

import com.rbkmoney.damsel.analytics.SubError;
import org.junit.Assert;
import org.junit.Test;

public class SubErrorGeneratorTest {

    @Test
    public void generateError() {
        SubError subError = SubErrorGenerator.generateError("authorization_failed:rejected_by_issuer");

        Assert.assertEquals(subError.code, "authorization_failed");
        Assert.assertEquals(subError.sub_error.code, "rejected_by_issuer");

        subError = SubErrorGenerator.generateError(null);

        Assert.assertEquals(subError.code, SubErrorGenerator.EMPTY_ERROR_CODE);
    }
}