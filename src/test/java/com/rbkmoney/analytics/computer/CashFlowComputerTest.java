package com.rbkmoney.analytics.computer;

import com.rbkmoney.analytics.domain.CashFlowResult;
import com.rbkmoney.analytics.utils.BuildUtils;
import org.junit.Before;
import org.junit.Test;

import static java.util.Collections.emptyList;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class CashFlowComputerTest {

    private CashFlowComputer cashFlowComputer;

    @Before
    public void setUp() {
        cashFlowComputer = new CashFlowComputer();
    }

    @Test
    public void shouldComputeCashFlowResult() {
        // Given - When
        CashFlowResult result = cashFlowComputer.compute(BuildUtils.createCashFlow(1000L, 100L));

        // Then
        assertThat(result.getAmount(), is(1000L));
        assertThat(result.getSystemFee(), is(100L));
        assertThat(result.getProviderFee(), is(20L));
        assertThat(result.getExternalFee(), is(10L));
        assertThat(result.getGuaranteeDeposit(), is(100L));
    }

    @Test
    public void shouldHandleNullCashFlow() {
        // Given - When
        CashFlowResult result = cashFlowComputer.compute(null);

        // Then
        assertThat(result.getAmount(), is(0L));
        assertThat(result.getSystemFee(), is(0L));
        assertThat(result.getProviderFee(), is(0L));
        assertThat(result.getExternalFee(), is(0L));
        assertThat(result.getGuaranteeDeposit(), is(0L));
    }

    @Test
    public void shouldHandleEmptyCashFlow() {
        // Given - When
        CashFlowResult result = cashFlowComputer.compute(emptyList());

        // Then
        assertThat(result.getAmount(), is(0L));
        assertThat(result.getSystemFee(), is(0L));
        assertThat(result.getProviderFee(), is(0L));
        assertThat(result.getExternalFee(), is(0L));
        assertThat(result.getGuaranteeDeposit(), is(0L));
    }
}