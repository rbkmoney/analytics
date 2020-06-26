package com.rbkmoney.analytics.listener.mapper.factory;

import com.rbkmoney.analytics.constant.PayoutAccountType;
import com.rbkmoney.analytics.constant.PayoutType;
import com.rbkmoney.analytics.dao.model.PayoutRow;
import com.rbkmoney.damsel.domain.InternationalBankAccount;
import com.rbkmoney.damsel.domain.InternationalBankDetails;
import com.rbkmoney.damsel.domain.RussianBankAccount;
import com.rbkmoney.damsel.payout_processing.*;
import com.rbkmoney.geck.common.util.TBaseUtil;
import com.rbkmoney.geck.common.util.TypeUtil;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
public class PayoutRowFactory {

    public PayoutRow create(
            Event event,
            Payout payoutCreated,
            String payoutId,
            PayoutStatus payoutStatus) {
        PayoutRow payoutRow = new PayoutRow();
        payoutRow.setPayoutId(payoutId);
        payoutRow.setEventTime(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));
        payoutRow.setPayoutId(payoutCreated.getId());
        payoutRow.setPartyId(payoutCreated.getPartyId());
        payoutRow.setShopId(payoutCreated.getShopId());
        payoutRow.setContractId(payoutCreated.getContractId());
        payoutRow.setPayoutTime(TypeUtil.stringToLocalDateTime(payoutCreated.getCreatedAt()));
        payoutRow.setStatus(TBaseUtil.unionFieldToEnum(payoutStatus, com.rbkmoney.analytics.constant.PayoutStatus.class));
        payoutRow.setAmount(payoutCreated.getAmount());
        payoutRow.setFee(payoutCreated.getFee());
        payoutRow.setCurrency(payoutCreated.getCurrency().getSymbolicCode());
        payoutRow.setPayoutType(TBaseUtil.unionFieldToEnum(payoutCreated.getType(), PayoutType.class));

        if (payoutCreated.getType().isSetWallet()) {
            fillWalletDetails(payoutCreated, payoutRow);
        }

        if (payoutCreated.getType().isSetBankAccount()) {
            fillBankAccountDetails(payoutCreated, payoutRow);
        }

        if (payoutStatus.isSetCancelled()) {
            PayoutCancelled cancelled = payoutStatus.getCancelled();
            payoutRow.setStatusCancelledDetails(cancelled.getDetails());
        }

        return payoutRow;
    }

    private void fillWalletDetails(Payout payoutCreated, PayoutRow payoutRow) {
        String walletId = payoutCreated.getType().getWallet().getWalletId();
        payoutRow.setWalletId(walletId);
    }

    private void fillBankAccountDetails(Payout payoutCreated, PayoutRow payoutRow) {
        PayoutAccount payoutAccount = payoutCreated.getType().getBankAccount();
        payoutRow.setAccountType(TBaseUtil.unionFieldToEnum(payoutAccount, PayoutAccountType.class));

        if (payoutAccount.isSetRussianPayoutAccount()) {
            fillRussianBankAccountDetails(payoutRow, payoutAccount);
        }

        if (payoutAccount.isSetInternationalPayoutAccount()) {
            fillInternationalBankAccountDetails(payoutRow, payoutAccount);
        }
    }

    private void fillRussianBankAccountDetails(PayoutRow payoutRow, PayoutAccount payoutAccount) {
        RussianPayoutAccount russianPayoutAccount = payoutAccount.getRussianPayoutAccount();
        RussianBankAccount russianBankAccount = russianPayoutAccount.getBankAccount();
        payoutRow.setTypeAccountRussianAccount(russianBankAccount.getAccount());
        payoutRow.setTypeAccountRussianBankName(russianBankAccount.getBankName());
        payoutRow.setTypeAccountRussianBankPostAccount(russianBankAccount.getBankPostAccount());
        payoutRow.setTypeAccountRussianBankBik(russianBankAccount.getBankBik());
        payoutRow.setTypeAccountRussianInn(russianPayoutAccount.getInn());
        payoutRow.setTypeAccountPurpose(russianPayoutAccount.getPurpose());
        payoutRow.setTypeAccountLegalAgreementSignedAt(
                TypeUtil.stringToLocalDateTime(russianPayoutAccount.getLegalAgreement().getSignedAt()));
        payoutRow.setTypeAccountLegalAgreementId(russianPayoutAccount.getLegalAgreement().getLegalAgreementId());

        if (russianPayoutAccount.getLegalAgreement().isSetValidUntil()) {
            payoutRow.setTypeAccountLegalAgreementValidUntil(
                    TypeUtil.stringToLocalDateTime(russianPayoutAccount.getLegalAgreement().getValidUntil()));
        }
    }

    private void fillInternationalBankAccountDetails(PayoutRow payoutRow, PayoutAccount payoutAccount) {
        InternationalPayoutAccount internationalPayoutAccount = payoutAccount.getInternationalPayoutAccount();
        InternationalBankAccount bankAccount = internationalPayoutAccount.getBankAccount();
        payoutRow.setTypeAccountInternationalAccountHolder(bankAccount.getAccountHolder());
        payoutRow.setTypeAccountInternationalIban(bankAccount.getIban());
        payoutRow.setTypeAccountInternationalBankNumber(bankAccount.getNumber());

        if (bankAccount.isSetBank()) {
            InternationalBankDetails bankDetails = bankAccount.getBank();
            payoutRow.setTypeAccountInternationalBankName(bankDetails.getName());
            payoutRow.setTypeAccountInternationalBankAddress(bankDetails.getAddress());
            payoutRow.setTypeAccountInternationalBic(bankDetails.getBic());
            payoutRow.setTypeAccountInternationalBankAbaRtn(bankDetails.getAbaRtn());
            payoutRow.setTypeAccountInternationalBankCountryCode(
                    Optional.ofNullable(bankDetails.getCountry()).map(Enum::toString).orElse(null));
        }

        if (bankAccount.isSetCorrespondentAccount()) {
            InternationalBankAccount correspondentBankAccount = bankAccount.getCorrespondentAccount();
            payoutRow.setTypeAccountInternationalCorrespondentBankAccount(correspondentBankAccount.getAccountHolder());
            payoutRow.setTypeAccountInternationalCorrespondentBankIban(correspondentBankAccount.getIban());
            payoutRow.setTypeAccountInternationalCorrespondentBankNumber(correspondentBankAccount.getNumber());

            if (correspondentBankAccount.isSetBank()) {
                InternationalBankDetails correspondentBankDetails = correspondentBankAccount.getBank();
                payoutRow.setTypeAccountInternationalCorrespondentBankName(correspondentBankDetails.getName());
                payoutRow.setTypeAccountInternationalCorrespondentBankAddress(correspondentBankDetails.getAddress());
                payoutRow.setTypeAccountInternationalCorrespondentBankBic(correspondentBankDetails.getBic());
                payoutRow.setTypeAccountInternationalCorrespondentBankAbaRtn(correspondentBankDetails.getAbaRtn());
                payoutRow.setTypeAccountInternationalCorrespondentBankCountryCode(
                        Optional.ofNullable(correspondentBankDetails.getCountry()).map(Enum::toString).orElse(null));
            }
        }

        payoutRow.setTypeAccountInternationalLegalEntityLegalName(internationalPayoutAccount.getLegalEntity().getLegalName());
        payoutRow.setTypeAccountInternationalLegalEntityTradingName(internationalPayoutAccount.getLegalEntity().getTradingName());
        payoutRow.setTypeAccountInternationalLegalEntityRegisteredAddress(internationalPayoutAccount.getLegalEntity().getRegisteredAddress());
        payoutRow.setTypeAccountInternationalLegalEntityActualAddress(internationalPayoutAccount.getLegalEntity().getActualAddress());
        payoutRow.setTypeAccountInternationalLegalEntityRegisteredNumber(internationalPayoutAccount.getLegalEntity().getRegisteredNumber());
        payoutRow.setTypeAccountPurpose(internationalPayoutAccount.getPurpose());
        payoutRow.setTypeAccountLegalAgreementSignedAt(
                TypeUtil.stringToLocalDateTime(internationalPayoutAccount.getLegalAgreement().getSignedAt()));
        payoutRow.setTypeAccountLegalAgreementId(internationalPayoutAccount.getLegalAgreement().getLegalAgreementId());

        if (internationalPayoutAccount.getLegalAgreement().isSetValidUntil()) {
            payoutRow.setTypeAccountLegalAgreementValidUntil(
                    TypeUtil.stringToLocalDateTime(internationalPayoutAccount.getLegalAgreement().getValidUntil()));
        }
    }
}
