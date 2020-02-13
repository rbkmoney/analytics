package com.rbkmoney.analytics.repository;

import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;
import java.util.Map;

@Slf4j
@RunWith(SpringRunner.class)
public class AnalyticsApplicationTest extends ClickhouseAbstractTest {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    public void testAmount() {
        long sum = jdbcTemplate.queryForObject(
                "SELECT shopId, sum(amount) as sum " +
                        "from analytic.events_sink where status = 'captured'" +
                        "group by partyId, shopId, currency " +
                        "having shopId = 'ad8b7bfd-0760-4781-a400-51903ee8e501' and currency = 'RUB'",
                (resultSet, i) -> resultSet.getLong("sum"));
        Assert.assertEquals(5000L, sum);

        sum = jdbcTemplate.queryForObject(
                "SELECT partyId, sum(amount) as sum " +
                        "from analytic.events_sink where status = 'captured' " +
                        "group by partyId, currency " +
                        "having partyId = 'ca2e9162-eda2-4d17-bbfa-dc5e39b1772f' and currency = 'RUB'",
                (resultSet, i) -> resultSet.getLong("sum"));
        Assert.assertEquals(55000L, sum);

        sum = jdbcTemplate.queryForObject(
                "SELECT partyId, avg(amount) as sum " +
                        "from analytic.events_sink where status = 'captured' " +
                        "group by partyId, currency " +
                        "having partyId = 'ca2e9162-eda2-4d17-bbfa-dc5e39b1772f' and currency = 'RUB'",
                (resultSet, i) -> resultSet.getLong("sum"));
        Assert.assertEquals(27500L, sum);
    }

    @Test
    public void testCount() {
        long sum = jdbcTemplate.queryForObject(
                "SELECT partyId, uniq(invoiceId, paymentId) as sum " +
                        "from analytic.events_sink where status = 'captured'" +
                        "group by partyId, currency " +
                        "having partyId = 'ca2e9162-eda2-4d17-bbfa-dc5e39b1772f' and currency = 'RUB'",
                (resultSet, i) -> resultSet.getLong("sum"));
        Assert.assertEquals(2L, sum);
    }

    @Test
    public void testCountCurrent() {
        long sum = jdbcTemplate.queryForObject(
                "SELECT partyId, sum(amount * sign) as sum " +
                        "from analytic.events_sink " +
                        "group by partyId, currency " +
                        "having partyId = 'ca2e9162-eda2-4d17-bbfa-dc5e39b1772a' and currency = 'RUB' AND sum(sign) > 0",
                (resultSet, i) -> resultSet.getLong("sum"));
        Assert.assertEquals(6000L, sum);
    }

    @Test
    public void testStatusListPayment() {
        List<Map<String, Object>> list = jdbcTemplate.queryForList(
                "SELECT partyId, status, sum(sign) as cnt " +
                        "from analytic.events_sink " +
                        "group by partyId, currency, status " +
                        "having partyId = 'ca2e9162-eda2-4d17-bbfa-dc5e39b1772a' and currency = 'RUB' AND sum(sign) > 0");

        list.forEach(stringObjectMap -> {
                    Object cnt = stringObjectMap.get("cnt");
                    Assert.assertEquals(2L, cnt);
                }
        );
    }

    @Test
    public void testAmountListPayment() {
        List<Map<String, Object>> list = jdbcTemplate.queryForList(
                "SELECT partyId, status, sum(amount * sign) as sum " +
                        "from analytic.events_sink " +
                        "group by partyId, currency, status " +
                        "having partyId = 'ca2e9162-eda2-4d17-bbfa-dc5e39b1772a' and currency = 'RUB' and status in('captured', 'processed') " +
                        "AND sum(sign) > 0");

        list.forEach(stringObjectMap -> {
                    Object cnt = stringObjectMap.get("sum");
                    Assert.assertEquals(6000L, cnt);
                }
        );
    }

    @Test
    public void testPaymentTool() {
        List<Map<String, Object>> list = jdbcTemplate.queryForList(
                "SELECT partyId, paymentTool," +
                        "( SELECT sum(sign) from analytic.events_sink " +
                        "group by partyId, currency " +
                        "having partyId = 'ca2e9162-eda2-4d17-bbfa-dc5e39b1772a' and currency = 'RUB' " +
                        "AND sum(sign) > 0) as total_count, " +
                        "sum(sign) * 100 / total_count as sum " +
                        "from analytic.events_sink " +
                        "group by partyId, currency, paymentTool " +
                        "having partyId = 'ca2e9162-eda2-4d17-bbfa-dc5e39b1772a' and currency = 'RUB' " +
                        "AND sum(sign) > 0");

        list.forEach(stringObjectMap -> {
                    Object cnt = stringObjectMap.get("sum");
                    Assert.assertEquals(50.0, cnt);
                    System.out.println(stringObjectMap);
                }
        );
    }

    @Test
    public void testErrorReason() {
        List<Map<String, Object>> list = jdbcTemplate.queryForList(
                "SELECT partyId, errorReason," +
                        "( SELECT sum(sign) from analytic.events_sink " +
                        "group by partyId,status, currency " +
                        "having partyId = 'ca2e9162-eda2-4d17-bbfa-dc5e39b1772a' and currency = 'RUB' and status = 'failed' " +
                        "AND sum(sign) > 0) as total_count, " +
                        "sum(sign) * 100 / total_count as sum " +
                        "from analytic.events_sink " +
                        "group by partyId, status, currency, errorReason " +
                        "having partyId = 'ca2e9162-eda2-4d17-bbfa-dc5e39b1772a' and currency = 'RUB' and status = 'failed' " +
                        "AND sum(sign) > 0");

        list.forEach(stringObjectMap -> {
                    Object cnt = stringObjectMap.get("sum");
                    Assert.assertEquals(100.0, cnt);
                    System.out.println(stringObjectMap);
                }
        );
    }
}
