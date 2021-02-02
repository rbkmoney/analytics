package com.rbkmoney.analytics.service;

import com.rbkmoney.analytics.AnalyticsApplication;
import com.rbkmoney.analytics.dao.repository.postgres.party.management.DominantDao;
import com.rbkmoney.analytics.utils.KafkaAbstractTest;
import com.rbkmoney.analytics.utils.TestData;
import com.rbkmoney.damsel.domain.CategoryType;
import com.rbkmoney.damsel.domain_config.Commit;
import com.rbkmoney.damsel.domain_config.RepositorySrv;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.testcontainers.containers.PostgreSQLContainer;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.*;

@Slf4j
@RunWith(SpringRunner.class)
@SpringBootTest(classes = AnalyticsApplication.class,
        properties = {"kafka.state.cache.size=0"})
@ContextConfiguration(initializers = {DominantServiceTest.Initializer.class})
public class DominantServiceTest extends KafkaAbstractTest {

    @ClassRule
    @SuppressWarnings("rawtypes")
    public static PostgreSQLContainer postgres = (PostgreSQLContainer) new PostgreSQLContainer("postgres:9.6")
            .withStartupTimeout(Duration.ofMinutes(5));

    public static class Initializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {
        @Override
        public void initialize(ConfigurableApplicationContext configurableApplicationContext) {
            TestPropertyValues.of(
                    "postgres.db.url=" + postgres.getJdbcUrl(),
                    "postgres.db.user=" + postgres.getUsername(),
                    "postgres.db.password=" + postgres.getPassword(),
                    "spring.flyway.url=" + postgres.getJdbcUrl(),
                    "spring.flyway.user=" + postgres.getUsername(),
                    "spring.flyway.password=" + postgres.getPassword(),
                    "spring.flyway.enabled=true")
                    .applyTo(configurableApplicationContext.getEnvironment());
            postgres.start();
        }
    }

    @MockBean
    private RepositorySrv.Iface dominantClient;

    @Autowired
    private DominantService dominantService;

    @Autowired
    private DominantDao dominantDao;

    @Before
    public void setUp() throws Exception {
        Commit firstCommit = TestData.buildInsertCategoryCommit(64, "testName", "testDescription", CategoryType.test);
        Commit secondCommit = TestData.buildUpdateCategoryCommit(64, "testNameNew", "testDescriptionNew", CategoryType.live, firstCommit.getOps().get(0).getInsert().getObject());
        Map<Long, Commit> commitMap = new HashMap<>();
        commitMap.put(1L, firstCommit);
        commitMap.put(2L, secondCommit);
        when(dominantClient.pullRange(anyLong(),anyInt())).thenReturn(commitMap);
    }

    @Test
    public void testDominantVersion() {
        dominantService.pullDominantRange(10);
        Assert.assertEquals(2, (long) dominantDao.getLastVersion());
    }

}
