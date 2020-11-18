package com.rbkmoney.analytics.constant;

import com.rbkmoney.geck.filter.Condition;
import com.rbkmoney.geck.filter.Filter;
import com.rbkmoney.geck.filter.PathConditionFilter;
import com.rbkmoney.geck.filter.condition.IsNullCondition;
import com.rbkmoney.geck.filter.rule.PathConditionRule;

public enum EventType {

    INVOICE_PAYMENT_STATUS_CHANGED("invoice_payment_change.payload.invoice_payment_status_changed", new IsNullCondition().not()),
    INVOICE_PAYMENT_ADJUSTMENT_STATUS_CHANGED("invoice_payment_change.payload.invoice_payment_adjustment_change.payload.invoice_payment_adjustment_status_changed.status.captured", new IsNullCondition().not()),
    INVOICE_PAYMENT_REFUND_STATUS_CHANGED("invoice_payment_change.payload.invoice_payment_refund_change.payload.invoice_payment_refund_status_changed", new IsNullCondition().not()),
    INVOICE_PAYMENT_CHARGEBACK_STATUS_CHANGED("invoice_payment_change.payload.invoice_payment_chargeback_change.payload.invoice_payment_chargeback_status_changed", new IsNullCondition().not()),
    INVOICE_PAYMENT_SESSION_CHANGED("invoice_payment_change.payload.invoice_payment_session_change.payload.session_transaction_bound", new IsNullCondition().not()),
    INVOICE_PAYMENT_RISK_SCORE_CHANGED("invoice_payment_change.payload.invoice_payment_risk_score_changed", new IsNullCondition().not()),
    PAYOUT_STATUS_CHANGED("payout_status_changed", new IsNullCondition().not()),
    PARTY_CREATED("party_created", new IsNullCondition().not()),
    PARTY_BLOCKING("party_blocking", new IsNullCondition().not()),
    REVISION_CHANGED("revision_changed", new IsNullCondition().not()),
    PARTY_SUSPENSION("party_suspension", new IsNullCondition().not()),
    SHOP_BLOCKING("shop_blocking", new IsNullCondition().not()),
    SHOP_SUSPENSION("shop_suspension", new IsNullCondition().not()),
    PARTY_CLAIM_CREATED("claim_created", new IsNullCondition().not());

    Filter filter;

    EventType(String path, Condition... conditions) {
        this.filter = new PathConditionFilter(new PathConditionRule(path, conditions));
    }

    public Filter getFilter() {
        return filter;
    }

}
