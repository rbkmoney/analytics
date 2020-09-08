package com.rbkmoney.analytics.computer;

import com.rbkmoney.analytics.domain.CashFlowResult;
import com.rbkmoney.damsel.domain.*;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.List;

@Service
public class ReversedCashFlowComputer {

    public CashFlowResult compute(List<FinalCashFlowPosting> reversedCashFlow) {
        long amount = 0L;
        long systemFee = 0L;
        long providerFee = 0L;
        long externalFee = 0L;
        long guaranteeDeposit = 0L;

        if (CollectionUtils.isEmpty(reversedCashFlow)) {
            return CashFlowResult.EMPTY;
        }

        for (FinalCashFlowPosting posting : reversedCashFlow) {
            if (!posting.isSetSource() || !posting.isSetDestination()) {
                continue;
            }

            if (isReversedPayment(posting)) {
                amount += posting.getVolume().getAmount();
            }

            if (isReversedSystemFee(posting)) {
                systemFee += posting.getVolume().getAmount();
            }

            if (isReversedProviderFee(posting)) {
                providerFee += posting.getVolume().getAmount();
            }

            if (isReversedExternalFee(posting)) {
                externalFee += posting.getVolume().getAmount();
            }

            if (isReversedGuaranteeDeposit(posting)) {
                guaranteeDeposit += posting.getVolume().getAmount();
            }

        }

        return CashFlowResult.builder()
                .amount(amount)
                .systemFee(systemFee)
                .providerFee(providerFee)
                .externalFee(externalFee)
                .guaranteeDeposit(guaranteeDeposit)
                .build();
    }

    private boolean isReversedPayment(FinalCashFlowPosting posting) {
        return posting.getDestination().getAccountType().isSetProvider() && isSettlement(posting.getDestination())
                && posting.getSource().getAccountType().isSetMerchant() && isSettlement(posting.getSource());
    }

    private boolean isReversedRefund(FinalCashFlowPosting posting) {
        return posting.getDestination().getAccountType().isSetMerchant() && isSettlement(posting.getDestination())
                && posting.getSource().getAccountType().isSetProvider() && isSettlement(posting.getSource());
    }

    private boolean isReversedSystemFee(FinalCashFlowPosting posting) {
        return posting.getDestination().getAccountType().isSetMerchant() && isSettlement(posting.getDestination())
                && posting.getSource().getAccountType().isSetSystem() && isSettlement(posting.getSource());
    }

    private boolean isReversedProviderFee(FinalCashFlowPosting posting) {
        return posting.getDestination().getAccountType().isSetSystem() && isSettlement(posting.getDestination())
                && posting.getSource().getAccountType().isSetProvider() && isSettlement(posting.getSource());
    }

    private boolean isReversedExternalFee(FinalCashFlowPosting posting) {
        return posting.getDestination().getAccountType().isSetSystem() && isSettlement(posting.getDestination())
                && posting.getSource().getAccountType().isSetExternal() && isExternal(posting.getSource());
    }

    private boolean isReversedGuaranteeDeposit(FinalCashFlowPosting posting) {
        return posting.getDestination().getAccountType().isSetMerchant() && isSettlement(posting.getDestination())
                && posting.getSource().getAccountType().isSetMerchant() && isGuarantee(posting.getSource());
    }

    private boolean isSettlement(FinalCashFlowAccount account) {
        return (account.getAccountType().isSetMerchant() && account.getAccountType().getMerchant() == MerchantCashFlowAccount.settlement)
                || (account.getAccountType().isSetProvider() && account.getAccountType().getProvider() == ProviderCashFlowAccount.settlement)
                || (account.getAccountType().isSetSystem() && account.getAccountType().getSystem() == SystemCashFlowAccount.settlement);
    }

    private boolean isExternal(FinalCashFlowAccount account) {
        return account.getAccountType().getExternal() == ExternalCashFlowAccount.income
                || account.getAccountType().getExternal() == ExternalCashFlowAccount.outcome;
    }

    private boolean isGuarantee(FinalCashFlowAccount account) {
        return account.getAccountType().getMerchant() == MerchantCashFlowAccount.guarantee;
    }
}