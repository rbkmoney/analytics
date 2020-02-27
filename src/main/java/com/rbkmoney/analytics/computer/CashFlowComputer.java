package com.rbkmoney.analytics.computer;

import com.rbkmoney.analytics.domain.CashFlowResult;
import com.rbkmoney.damsel.domain.*;

import java.util.List;

import static org.msgpack.core.Preconditions.checkState;

public class CashFlowComputer {

    public static CashFlowResult compute(List<FinalCashFlowPosting> cashFlow) {
        long accountId = -1;
        long merchantAmount = 0L;
        long systemFee = 0L;
        long providerFee = 0L;
        long externalFee = 0L;
        long guaranteeDeposit = 0L;

        if (cashFlow != null) {
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
        }
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

    private static boolean isPayment(FinalCashFlowPosting posting) {
        return posting.getSource().getAccountType().isSetProvider() && isSettlement(posting.getSource())
                && posting.getDestination().getAccountType().isSetMerchant() && isSettlement(posting.getDestination());
    }

    private static boolean isRefund(FinalCashFlowPosting posting) {
        return posting.getSource().getAccountType().isSetMerchant() && isSettlement(posting.getSource())
                && posting.getDestination().getAccountType().isSetProvider() && isSettlement(posting.getDestination());
    }

    private static boolean isSystemFee(FinalCashFlowPosting posting) {
        return posting.getSource().getAccountType().isSetMerchant() && isSettlement(posting.getSource())
                && posting.getDestination().getAccountType().isSetSystem() && isSettlement(posting.getDestination());
    }

    private static boolean isProviderFee(FinalCashFlowPosting posting) {
        return posting.getSource().getAccountType().isSetSystem() && isSettlement(posting.getSource())
                && posting.getDestination().getAccountType().isSetProvider() && isSettlement(posting.getDestination());
    }

    private static boolean isExternalFee(FinalCashFlowPosting posting) {
        return posting.getSource().getAccountType().isSetSystem() && isSettlement(posting.getSource())
                && posting.getDestination().getAccountType().isSetExternal() && isExternal(posting.getDestination());
    }

    private static boolean isGuaranteeDeposit(FinalCashFlowPosting posting) {
        return posting.getSource().getAccountType().isSetMerchant() && isSettlement(posting.getSource())
                && posting.getDestination().getAccountType().isSetMerchant() && isGuarantee(posting.getDestination());
    }

    private static boolean isSettlement(FinalCashFlowAccount account) {
        return (account.getAccountType().isSetMerchant() && account.getAccountType().getMerchant() == MerchantCashFlowAccount.settlement)
                || (account.getAccountType().isSetProvider() && account.getAccountType().getProvider() == ProviderCashFlowAccount.settlement)
                || (account.getAccountType().isSetSystem() && account.getAccountType().getSystem() == SystemCashFlowAccount.settlement);
    }

    private static boolean isExternal(FinalCashFlowAccount account) {
        return account.getAccountType().getExternal() == ExternalCashFlowAccount.income
                || account.getAccountType().getExternal() == ExternalCashFlowAccount.outcome;
    }

    private static boolean isGuarantee(FinalCashFlowAccount account) {
        return account.getAccountType().getMerchant() == MerchantCashFlowAccount.guarantee;
    }
}