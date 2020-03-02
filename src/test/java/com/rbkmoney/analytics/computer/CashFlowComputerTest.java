package com.rbkmoney.analytics.computer;

import com.rbkmoney.analytics.domain.CashFlowResult;
import com.rbkmoney.analytics.utils.BuildUtils;
import com.rbkmoney.damsel.domain.*;
import org.junit.Before;
import org.junit.Test;

import java.util.Optional;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

@SuppressWarnings("SameParameterValue")
public class CashFlowComputerTest {


    private CashFlowComputer cashFlowComputer;

    @Before
    public void setUp() {
        cashFlowComputer = new CashFlowComputer();
    }

    @Test
    public void shouldComputeCashFlowResult() {

        // When
        Optional<CashFlowResult> compute = cashFlowComputer.compute(BuildUtils.createCashFlow(1000L, 100L));

        CashFlowResult result = compute.get();
        // Then
        assertThat(result.getAccountId(), is(1L));
        assertThat(result.getTotalAmount(), is(1100L));
        assertThat(result.getMerchantAmount(), is(1000L));
        assertThat(result.getSystemFee(), is(100L));
        assertThat(result.getProviderFee(), is(20L));
        assertThat(result.getExternalFee(), is(10L));
        assertThat(result.getGuaranteeDeposit(), is(100L));
    }

    private FinalCashFlowPosting payment(long amount) {
        return new FinalCashFlowPosting()
                .setSource(
                        new FinalCashFlowAccount()
                                .setAccountType(
                                        new CashFlowAccount(
                                                CashFlowAccount.provider(
                                                        ProviderCashFlowAccount.settlement))))
                .setDestination(
                        new FinalCashFlowAccount()
                                .setAccountId(1L)
                                .setAccountType(
                                        new CashFlowAccount(
                                                CashFlowAccount.merchant(
                                                        MerchantCashFlowAccount.settlement))))
                .setVolume(new Cash()
                        .setAmount(amount));
    }

    private FinalCashFlowPosting refund(long amount) {
        return new FinalCashFlowPosting()
                .setSource(
                        new FinalCashFlowAccount()
                                .setAccountId(1L)
                                .setAccountType(
                                        new CashFlowAccount(
                                                CashFlowAccount.merchant(
                                                        MerchantCashFlowAccount.settlement))))
                .setDestination(
                        new FinalCashFlowAccount()
                                .setAccountType(
                                        new CashFlowAccount(
                                                CashFlowAccount.provider(
                                                        ProviderCashFlowAccount.settlement))))
                .setVolume(new Cash()
                        .setAmount(amount));
    }

    private FinalCashFlowPosting systemFee(long amount) {
        return new FinalCashFlowPosting()
                .setSource(
                        new FinalCashFlowAccount()
                                .setAccountType(
                                        new CashFlowAccount(
                                                CashFlowAccount.merchant(
                                                        MerchantCashFlowAccount.settlement))))
                .setDestination(
                        new FinalCashFlowAccount()
                                .setAccountId(1L)
                                .setAccountType(
                                        new CashFlowAccount(
                                                CashFlowAccount.system(
                                                        SystemCashFlowAccount.settlement))))
                .setVolume(new Cash()
                        .setAmount(amount));
    }

    private FinalCashFlowPosting providerFee(long amount) {
        return new FinalCashFlowPosting()
                .setSource(
                        new FinalCashFlowAccount()
                                .setAccountType(
                                        new CashFlowAccount(
                                                CashFlowAccount.system(
                                                        SystemCashFlowAccount.settlement))))
                .setDestination(
                        new FinalCashFlowAccount()
                                .setAccountId(1L)
                                .setAccountType(
                                        new CashFlowAccount(
                                                CashFlowAccount.provider(
                                                        ProviderCashFlowAccount.settlement))))
                .setVolume(new Cash()
                        .setAmount(amount));
    }

    private FinalCashFlowPosting externalFee(long amount) {
        return new FinalCashFlowPosting()
                .setSource(
                        new FinalCashFlowAccount()
                                .setAccountType(
                                        new CashFlowAccount(
                                                CashFlowAccount.system(
                                                        SystemCashFlowAccount.settlement))))
                .setDestination(
                        new FinalCashFlowAccount()
                                .setAccountId(1L)
                                .setAccountType(
                                        new CashFlowAccount(
                                                CashFlowAccount.external(
                                                        ExternalCashFlowAccount.outcome))))
                .setVolume(new Cash()
                        .setAmount(amount));
    }

    private FinalCashFlowPosting guaranteeDeposit(long amount) {
        return new FinalCashFlowPosting()
                .setSource(
                        new FinalCashFlowAccount()
                                .setAccountType(
                                        new CashFlowAccount(
                                                CashFlowAccount.merchant(
                                                        MerchantCashFlowAccount.settlement))))
                .setDestination(
                        new FinalCashFlowAccount()
                                .setAccountId(1L)
                                .setAccountType(
                                        new CashFlowAccount(
                                                CashFlowAccount.merchant(
                                                        MerchantCashFlowAccount.guarantee))))
                .setVolume(new Cash()
                        .setAmount(amount));
    }


    private FinalCashFlowPosting incorrectPosting(long amount) {
        return new FinalCashFlowPosting()
                .setDestination(
                        new FinalCashFlowAccount()
                                .setAccountId(1L)
                                .setAccountType(
                                        new CashFlowAccount(
                                                CashFlowAccount.merchant(
                                                        MerchantCashFlowAccount.settlement))))
                .setVolume(new Cash()
                        .setAmount(amount));
    }

}