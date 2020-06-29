package com.rbkmoney.analytics.repository;

import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.junit4.SpringRunner;

import java.math.BigInteger;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@Slf4j
@RunWith(SpringRunner.class)
public class AnalyticsApplicationTest extends ClickHouseAbstractTest {

    @Autowired
    private JdbcTemplate clickHouseJdbcTemplate;

    @Test
    public void testAmount() {
        long sum = clickHouseJdbcTemplate.queryForObject(
                "SELECT shopId, sum(amount) as sum " +
                        "from analytic.events_sink where status = 'captured'" +
                        "group by partyId, shopId, currency " +
                        "having shopId = 'ad8b7bfd-0760-4781-a400-51903ee8e501' and currency = 'RUB'",
                (resultSet, i) -> resultSet.getLong("sum"));
        assertEquals(5000L, sum);

        sum = clickHouseJdbcTemplate.queryForObject(
                "SELECT partyId, sum(amount) as sum " +
                        "from analytic.events_sink where status = 'captured' " +
                        "group by partyId, currency " +
                        "having partyId = 'ca2e9162-eda2-4d17-bbfa-dc5e39b1772f' and currency = 'RUB'",
                (resultSet, i) -> resultSet.getLong("sum"));
        assertEquals(55000L, sum);

        sum = clickHouseJdbcTemplate.queryForObject(
                "SELECT partyId, avg(amount) as sum " +
                        "from analytic.events_sink where status = 'captured' " +
                        "group by partyId, currency " +
                        "having partyId = 'ca2e9162-eda2-4d17-bbfa-dc5e39b1772f' and currency = 'RUB'",
                (resultSet, i) -> resultSet.getLong("sum"));
        assertEquals(27500L, sum);
    }

    @Test
    public void testCount() {
        long sum = clickHouseJdbcTemplate.queryForObject(
                "SELECT partyId, uniq(invoiceId, paymentId) as sum " +
                        "from analytic.events_sink where status = 'captured'" +
                        "group by partyId, currency " +
                        "having partyId = 'ca2e9162-eda2-4d17-bbfa-dc5e39b1772f' and currency = 'RUB'",
                (resultSet, i) -> resultSet.getLong("sum"));
        assertEquals(2L, sum);
    }

    @Test
    public void testCountCurrent() {
        long sum = clickHouseJdbcTemplate.queryForObject(
                "SELECT partyId, sum(amount) as sum " +
                        "from analytic.events_sink " +
                        "group by partyId, currency " +
                        "having partyId = 'ca2e9162-eda2-4d17-bbfa-dc5e39b1772a' and currency = 'RUB'",
                (resultSet, i) -> resultSet.getLong("sum"));
        assertEquals(7000L, sum);
    }

    @Test
    public void testStatusListPayment() {
        List<Map<String, Object>> list = clickHouseJdbcTemplate.queryForList(
                "SELECT partyId, status, count() as cnt " +
                        "from analytic.events_sink " +
                        "group by partyId, currency, status " +
                        "having partyId = 'ca2e9162-eda2-4d17-bbfa-dc5e39b1772a' and currency = 'RUB'");

        list.forEach(stringObjectMap -> {
                    Object cnt = stringObjectMap.get("cnt");
                    if (stringObjectMap.get("status").equals("captured"))
                        assertEquals(2L, ((BigInteger) cnt).longValue());
                    else if (stringObjectMap.get("status").equals("failed"))
                        assertEquals(1L, ((BigInteger) cnt).longValue());
                }
        );
    }

    @Test
    public void testAmountListPayment() {
        List<Map<String, Object>> list = clickHouseJdbcTemplate.queryForList(
                "SELECT partyId, status, sum(amount) as sum " +
                        "from analytic.events_sink " +
                        "group by partyId, currency, status " +
                        "having partyId = 'ca2e9162-eda2-4d17-bbfa-dc5e39b1772a' and currency = 'RUB' and status in('captured', 'processed')");

        list.forEach(stringObjectMap -> {
                    Object cnt = stringObjectMap.get("sum");
                    assertEquals(6000L, ((BigInteger) cnt).longValue());
                }
        );
    }

    @Test
    public void testPaymentTool() {
        List<Map<String, Object>> list = clickHouseJdbcTemplate.queryForList(
                "SELECT partyId, paymentTool," +
                        "( SELECT count() from analytic.events_sink  where status='captured'" +
                        "group by partyId, currency " +
                        "having partyId = 'ca2e9162-eda2-4d17-bbfa-dc5e39b1772a' and currency = 'RUB') as total_count, " +
                        "count() * 100 / total_count as sum " +
                        "from analytic.events_sink where status='captured'" +
                        "group by partyId, currency, paymentTool " +
                        "having partyId = 'ca2e9162-eda2-4d17-bbfa-dc5e39b1772a' and currency = 'RUB'");

        list.forEach(stringObjectMap -> {
                    Object cnt = stringObjectMap.get("sum");
                    assertEquals(50.0, cnt);
                    System.out.println(stringObjectMap);
                }
        );
    }

    @Test
    public void testErrorReason() {
        List<Map<String, Object>> list = clickHouseJdbcTemplate.queryForList(
                "SELECT partyId, errorReason," +
                        "( SELECT count() from analytic.events_sink " +
                        "group by partyId,status, currency " +
                        "having partyId = 'ca2e9162-eda2-4d17-bbfa-dc5e39b1772a' and currency = 'RUB' and status = 'failed') as total_count, " +
                        "count() * 100 / total_count as sum " +
                        "from analytic.events_sink " +
                        "group by partyId, status, currency, errorReason " +
                        "having partyId = 'ca2e9162-eda2-4d17-bbfa-dc5e39b1772a' and currency = 'RUB' and status = 'failed'");

        list.forEach(stringObjectMap -> {
                    Object cnt = stringObjectMap.get("sum");
                    assertEquals(100.0, cnt);
                    System.out.println(stringObjectMap);
                }
        );
    }

    @Test
    public void testPayoutsAmount() {
        long sum = clickHouseJdbcTemplate.queryForObject(
                "SELECT shopId, sum(amount) as sum " +
                        "from analytic.events_sink_payout where status = 'paid' " +
                        "group by partyId, shopId, currency " +
                        "having shopId = 'ca2e9162-eda2-4d17-bbfa-dc5e39b1772f' and currency = 'RUB'",
                (resultSet, i) -> resultSet.getLong("sum"));
        assertEquals(10000, sum);
    }
}
