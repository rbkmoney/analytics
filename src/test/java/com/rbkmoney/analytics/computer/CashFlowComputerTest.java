package com.rbkmoney.analytics.computer;

import com.rbkmoney.analytics.domain.CashFlowResult;
import com.rbkmoney.analytics.utils.BuildUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.Optional;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class CashFlowComputerTest {

    private CashFlowComputer cashFlowComputer;

    @Before
    public void setUp() {
        cashFlowComputer = new CashFlowComputer();
    }

    @Test
    public void shouldComputeCashFlowResult() {
        // Given - When
        Optional<CashFlowResult> compute = cashFlowComputer.compute(BuildUtils.createCashFlow(1000L, 100L));

        // Then
        assertTrue(compute.isPresent());
        CashFlowResult result = compute.get();

        assertThat(result.getAmount(), is(1000L));
        assertThat(result.getSystemFee(), is(100L));
        assertThat(result.getProviderFee(), is(20L));
        assertThat(result.getExternalFee(), is(10L));
        assertThat(result.getGuaranteeDeposit(), is(100L));
    }
}