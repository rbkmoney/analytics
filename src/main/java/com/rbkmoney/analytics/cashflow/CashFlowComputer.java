package com.rbkmoney.analytics.cashflow;

import com.rbkmoney.damsel.domain.*;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;

public class CashFlowComputer {

    public CashFlowResult compute(List<FinalCashFlowPosting> cashFlow) {
        long accountId = -1;
        long merchantAmount = 0L;
        long systemFee = 0L;
        long providerFee = 0L;
        long externalFee = 0L;
        long guaranteeDeposit = 0L;

        for (FinalCashFlowPosting posting : cashFlow) {
            if (!posting.isSetSource() || !posting.isSetDestination()) {
                continue;
            }

            if (isPayment(posting)) {
                accountId = posting.getDestination().getAccountId();
                merchantAmount += posting.getVolume().getAmount();
            }

            if (isRefund(posting)) {
                accountId = posting.getSource().getAccountId();
                merchantAmount += posting.getVolume().getAmount();
            }

            if (isSystemFee(posting)) {
                systemFee += posting.getVolume().getAmount();
            }

            if (isProviderFee(posting)) {
                providerFee += posting.getVolume().getAmount();
            }

            if (isExternalFee(posting)) {
                externalFee += posting.getVolume().getAmount();
            }

            if (isGuaranteeDeposit(posting)) {
                guaranteeDeposit += posting.getVolume().getAmount();
            }
        }

        checkState(accountId > 0, "Unable to get correct accountId");

        return CashFlowResult.builder()
                .accountId(accountId)
                .totalAmount(merchantAmount + systemFee)
                .merchantAmount(merchantAmount)
                .systemFee(systemFee)
                .providerFee(providerFee)
                .externalFee(externalFee)
                .guaranteeDeposit(guaranteeDeposit)
                .build();
    }

    private boolean isPayment(FinalCashFlowPosting posting) {
        return posting.getSource().getAccountType().isSetProvider() && isSettlement(posting.getSource())
                && posting.getDestination().getAccountType().isSetMerchant() && isSettlement(posting.getDestination());
    }

    private boolean isRefund(FinalCashFlowPosting posting) {
        return posting.getSource().getAccountType().isSetMerchant() && isSettlement(posting.getSource())
                && posting.getDestination().getAccountType().isSetProvider() && isSettlement(posting.getDestination());
    }

    private boolean isSystemFee(FinalCashFlowPosting posting) {
        return posting.getSource().getAccountType().isSetMerchant() && isSettlement(posting.getSource())
                && posting.getDestination().getAccountType().isSetSystem() && isSettlement(posting.getDestination());
    }

    private boolean isProviderFee(FinalCashFlowPosting posting) {
        return posting.getSource().getAccountType().isSetSystem() && isSettlement(posting.getSource())
                && posting.getDestination().getAccountType().isSetProvider() && isSettlement(posting.getDestination());
    }

    private boolean isExternalFee(FinalCashFlowPosting posting) {
        return posting.getSource().getAccountType().isSetSystem() && isSettlement(posting.getSource())
                && posting.getDestination().getAccountType().isSetExternal() && isExternal(posting.getDestination());
    }

    private boolean isGuaranteeDeposit(FinalCashFlowPosting posting) {
        return posting.getSource().getAccountType().isSetMerchant() && isSettlement(posting.getSource())
                && posting.getDestination().getAccountType().isSetMerchant() && isGuarantee(posting.getDestination());
    }

    private boolean isSettlement(FinalCashFlowAccount account) {
        if (account.getAccountType().isSetMerchant()) {
            return account.getAccountType().getMerchant() == MerchantCashFlowAccount.settlement;
        }

        if (account.getAccountType().isSetProvider()) {
            return account.getAccountType().getProvider() == ProviderCashFlowAccount.settlement;
        }

        if (account.getAccountType().isSetSystem()) {
            return account.getAccountType().getSystem() == SystemCashFlowAccount.settlement;
        }

        throw new IllegalArgumentException("Incorrect accountType: " + account.getAccountType());
    }

    private boolean isExternal(FinalCashFlowAccount account) {
        return account.getAccountType().getExternal() == ExternalCashFlowAccount.income
                || account.getAccountType().getExternal() == ExternalCashFlowAccount.outcome;
    }

    private boolean isGuarantee(FinalCashFlowAccount account) {
        return account.getAccountType().getMerchant() == MerchantCashFlowAccount.guarantee;
    }
}
