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
        payoutRow.setStatus(TBaseUtil.unionFieldToEnum(payoutStatus,
                com.rbkmoney.analytics.constant.PayoutStatus.class)
        );
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
        payoutRow.setRussianAccount(russianBankAccount.getAccount());
        payoutRow.setRussianBankName(russianBankAccount.getBankName());
        payoutRow.setRussianBankPostAccount(russianBankAccount.getBankPostAccount());
        payoutRow.setRussianBankBik(russianBankAccount.getBankBik());
        payoutRow.setRussianInn(russianPayoutAccount.getInn());
        payoutRow.setPurpose(russianPayoutAccount.getPurpose());
        payoutRow.setLegalAgreementSignedAt(
                TypeUtil.stringToLocalDateTime(russianPayoutAccount.getLegalAgreement().getSignedAt()));
        payoutRow.setLegalAgreementId(russianPayoutAccount.getLegalAgreement().getLegalAgreementId());

        if (russianPayoutAccount.getLegalAgreement().isSetValidUntil()) {
            payoutRow.setLegalAgreementValidUntil(
                    TypeUtil.stringToLocalDateTime(russianPayoutAccount.getLegalAgreement().getValidUntil()));
        }
    }

    private void fillInternationalBankAccountDetails(PayoutRow payoutRow, PayoutAccount payoutAccount) {
        InternationalPayoutAccount internationalPayoutAccount = payoutAccount.getInternationalPayoutAccount();
        InternationalBankAccount bankAccount = internationalPayoutAccount.getBankAccount();
        payoutRow.setInternationalAccountHolder(bankAccount.getAccountHolder());
        payoutRow.setInternationalIban(bankAccount.getIban());
        payoutRow.setInternationalBankNumber(bankAccount.getNumber());

        if (bankAccount.isSetBank()) {
            InternationalBankDetails bankDetails = bankAccount.getBank();
            payoutRow.setInternationalBankName(bankDetails.getName());
            payoutRow.setInternationalBankAddress(bankDetails.getAddress());
            payoutRow.setInternationalBic(bankDetails.getBic());
            payoutRow.setInternationalBankAbaRtn(bankDetails.getAbaRtn());
            payoutRow.setInternationalBankCountryCode(
                    Optional.ofNullable(bankDetails.getCountry()).map(Enum::toString).orElse(null));
        }

        if (bankAccount.isSetCorrespondentAccount()) {
            InternationalBankAccount correspondentBankAccount = bankAccount.getCorrespondentAccount();
            payoutRow.setInternationalCorrespondentBankAccount(correspondentBankAccount.getAccountHolder());
            payoutRow.setInternationalCorrespondentBankIban(correspondentBankAccount.getIban());
            payoutRow.setInternationalCorrespondentBankNumber(correspondentBankAccount.getNumber());

            if (correspondentBankAccount.isSetBank()) {
                InternationalBankDetails correspondentBankDetails = correspondentBankAccount.getBank();
                payoutRow.setInternationalCorrespondentBankName(correspondentBankDetails.getName());
                payoutRow.setInternationalCorrespondentBankAddress(correspondentBankDetails.getAddress());
                payoutRow.setInternationalCorrespondentBankBic(correspondentBankDetails.getBic());
                payoutRow.setInternationalCorrespondentBankAbaRtn(correspondentBankDetails.getAbaRtn());
                payoutRow.setInternationalCorrespondentBankCountryCode(
                        Optional.ofNullable(correspondentBankDetails.getCountry()).map(Enum::toString).orElse(null));
            }
        }

        payoutRow.setInternationalLegalEntityLegalName(internationalPayoutAccount.getLegalEntity().getLegalName());
        payoutRow.setInternationalLegalEntityTradingName(internationalPayoutAccount.getLegalEntity().getTradingName());
        payoutRow.setInternationalLegalEntityRegisteredAddress(
                internationalPayoutAccount.getLegalEntity().getRegisteredAddress()
        );
        payoutRow.setInternationalLegalEntityActualAddress(
                internationalPayoutAccount.getLegalEntity().getActualAddress()
        );
        payoutRow.setInternationalLegalEntityRegisteredNumber(
                internationalPayoutAccount.getLegalEntity().getRegisteredNumber()
        );
        payoutRow.setPurpose(internationalPayoutAccount.getPurpose());
        payoutRow.setLegalAgreementSignedAt(
                TypeUtil.stringToLocalDateTime(internationalPayoutAccount.getLegalAgreement().getSignedAt()));
        payoutRow.setLegalAgreementId(internationalPayoutAccount.getLegalAgreement().getLegalAgreementId());

        if (internationalPayoutAccount.getLegalAgreement().isSetValidUntil()) {
            payoutRow.setLegalAgreementValidUntil(
                    TypeUtil.stringToLocalDateTime(internationalPayoutAccount.getLegalAgreement().getValidUntil()));
        }
    }
}
