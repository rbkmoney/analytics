SELECT
    shop_id,
    currency,
    sum_payment_without_refund - sum_payout_without_cancelled as num
FROM
    (
        SELECT
            shop_id,
            currency,
            sum_captured_payment - sum_succeeded_refund as sum_payment_without_refund
        FROM (
                 SELECT
                     shopId as shop_id,
                     currency,
                     sum(amount - systemFee - guaranteeDeposit) as sum_captured_payment
                 FROM analytic.events_sink
                 WHERE
                   ? >= timestamp
                   and status = 'captured'
                   and partyId = ?
                   %1$s
                   %2$s
                 GROUP BY shopId, currency
             ) ANY LEFT JOIN (
        SELECT
            shopId as shop_id,
            currency,
            sum(amount + systemFee) as sum_succeeded_refund
        FROM analytic.events_sink_refund
        WHERE
            ? >= timestamp
            and status = 'succeeded'
            and partyId = ?
            %1$s
            %2$s
        GROUP BY shopId, currency
    ) USING  shop_id, currency
    ) ANY LEFT JOIN (
    SELECT
        shop_id,
        currency,
        sum_paid_payout - sum_cancelled_after_paid_payout as sum_payout_without_cancelled
    FROM
    (
        SELECT
            shopId as shop_id,
            currency,
            sum(amount + fee) as sum_paid_payout
        FROM analytic.events_sink_payout
        WHERE
            ? >= timestamp
            and status = 'paid'
            and partyId = ?
            %1$s
            %2$s
        GROUP BY shopId, currency
    ) ANY LEFT JOIN (
        SELECT
            shopId as shop_id,
            currency,
            sum(amount + fee) as sum_cancelled_after_paid_payout
        FROM analytic.events_sink_payout
        WHERE
            ? >= timestamp
            and status = 'cancelled'
            and isCancelledAfterBeingPaid = 1
            and partyId = ?
            %1$s
            %2$s
        GROUP BY shopId, currency
    ) USING shop_id, currency
) USING  shop_id, currency
