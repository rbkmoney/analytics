SELECT
    sum(amount - systemFee - providerFee - externalFee) as num,
    currency
FROM analytic.events_sink
WHERE timestamp >= ?
  AND timestamp <= ?
  AND eventTimeHour >= ?
  AND eventTimeHour <= ?
  AND eventTime >= ?
  AND eventTime <= ?
  AND status = 'captured'
  AND partyId = ?
    %1$s
    %2$s
GROUP BY partyId, currency;
