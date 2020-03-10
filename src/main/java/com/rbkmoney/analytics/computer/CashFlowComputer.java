package com.rbkmoney.analytics.computer;

import com.rbkmoney.analytics.domain.CashFlowResult;
import com.rbkmoney.damsel.domain.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

import static org.msgpack.core.Preconditions.checkState;

@Slf4j
@Service
public class CashFlowComputer {

    public Optional<CashFlowResult> compute(List<FinalCashFlowPosting> cashFlow) {
        long accountId = -1;
        long amount = 0L;
        long systemFee = 0L;
        long providerFee = 0L;
        long externalFee = 0L;
        long guaranteeDeposit = 0L;

        if (cashFlow == null) {
            return Optional.empty();
        }

        for (FinalCashFlowPosting posting : cashFlow) {
            if (!posting.isSetSource() || !posting.isSetDestination()) {
                continue;
            }

            if (isPayment(posting)) {
                accountId = posting.getDestination().getAccountId();
                amount += posting.getVolume().getAmount();
            }

            if (isRefund(posting)) {
                accountId = posting.getSource().getAccountId();
                amount += posting.getVolume().getAmount();
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

            checkState(accountId > 0, "Unable to get correct accountId");
        }
        return Optional.ofNullable(CashFlowResult.builder()
                .accountId(accountId)
                .amount(amount)
                .systemFee(systemFee)
                .providerFee(providerFee)
                .externalFee(externalFee)
                .guaranteeDeposit(guaranteeDeposit)
                .build());
    }

    private boolean isPayment(FinalCashFlowPosting posting) {
        log.debug("CashFlowComputer isPayment posting: {}", posting);
        return posting.getSource().getAccountType().isSetProvider() && isSettlement(posting.getSource())
                && posting.getDestination().getAccountType().isSetMerchant() && isSettlement(posting.getDestination());
    }

    private boolean isRefund(FinalCashFlowPosting posting) {
        log.debug("CashFlowComputer isRefund posting: {}", posting);
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