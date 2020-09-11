SELECT currency, sum(amount) as num
from analytic.events_sink_refund
where timestamp >= ?
        AND timestamp <= ?
        AND eventTimeHour >= ?
        AND eventTimeHour <= ?
        AND eventTime >= ?
        AND eventTime <= ?
        AND status='succeeded'
        %1$s
        %2$s
group by partyId, currency
having partyId = ?