package com.rbkmoney.analytics.listener;

import com.rbkmoney.analytics.AnalyticsApplication;
import com.rbkmoney.analytics.dao.repository.postgres.CategoryDao;
import com.rbkmoney.analytics.service.DominantService;
import com.rbkmoney.analytics.utils.TestData;
import com.rbkmoney.damsel.domain.*;
import com.rbkmoney.damsel.domain_config.*;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.testcontainers.containers.PostgreSQLContainer;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@RunWith(SpringRunner.class)
@SpringBootTest(classes = AnalyticsApplication.class,
        properties = {"kafka.state.cache.size=0"})
@ContextConfiguration(initializers = {DominantListenerTest.Initializer.class})
public class DominantListenerTest extends KafkaAbstractTest {

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
                    "spring.flyway.enabled=true",
                    "service.dominant.scheduler.enabled=false")
                    .applyTo(configurableApplicationContext.getEnvironment());
            postgres.start();
        }
    }

    @Autowired
    private DominantService dominantService;

    @Autowired
    private CategoryDao categoryDao;

    @Autowired
    private JdbcTemplate postgresJdbcTemplate;

    @Before
    public void setUp() throws Exception {
        postgresJdbcTemplate.execute("TRUNCATE TABLE analytics.category");
        postgresJdbcTemplate.execute("TRUNCATE TABLE analytics.dominant");
    }

    @Test
    public void testDominantProcessCommitsInsert() {
        Integer categoryId = 64;
        String categoryName = "testName";
        String categoryDescription = "testDescription";
        CategoryType categoryType = CategoryType.test;

        Map<Long, Commit> commits = new HashMap<>();
        commits.put(1L, TestData.buildInsertCategoryCommit(categoryId, categoryName, categoryDescription, categoryType));
        for (Map.Entry<Long, Commit> commitEntry : commits.entrySet()) {
            dominantService.processCommits(commitEntry.getKey(), commitEntry);
        }
        com.rbkmoney.analytics.domain.db.tables.pojos.Category category = categoryDao.getCategory(64, 1L);

        Assert.assertEquals(categoryId, category.getCategoryId());
        Assert.assertEquals(categoryName, category.getName());
        Assert.assertEquals(categoryDescription, category.getDescription());
        Assert.assertEquals(categoryType.name(), category.getType());
    }

    @Test
    public void testDominantProcessCommitsUpdate() {
        Integer categoryId = 64;
        String categoryName = "testName";
        String categoryDescription = "testDescription";
        CategoryType categoryType = CategoryType.test;
        String updatedCategoryName = "testNameNew";
        String updatedCategoryDescription = "testDescriptionNew";

        Map<Long, Commit> commits = new HashMap<>();
        Commit firstCommit = TestData.buildInsertCategoryCommit(categoryId, categoryName, categoryDescription, categoryType);
        DomainObject oldObject = firstCommit.getOps().get(0).getInsert().getObject();
        Commit secondCommit = TestData.buildUpdateCategoryCommit(categoryId, updatedCategoryName, updatedCategoryDescription, categoryType, oldObject);
        commits.put(1L, firstCommit);
        commits.put(2L, secondCommit);

        for (Map.Entry<Long, Commit> commitEntry : commits.entrySet()) {
            dominantService.processCommits(commitEntry.getKey(), commitEntry);
        }
        com.rbkmoney.analytics.domain.db.tables.pojos.Category category = categoryDao.getCategory(64, 2L);

        Assert.assertEquals(categoryId, category.getCategoryId());
        Assert.assertEquals(updatedCategoryName, category.getName());
        Assert.assertEquals(updatedCategoryDescription, category.getDescription());
    }

    @Test
    public void testDominantProcessCommitsRemove() {
        Integer categoryId = 64;
        String categoryName = "testName";
        String categoryDescription = "testDescription";
        CategoryType categoryType = CategoryType.test;
        Map<Long, Commit> commits = new HashMap<>();
        Commit firstCommit = TestData.buildInsertCategoryCommit(categoryId, categoryName, categoryDescription, categoryType);
        Commit secondCommit = TestData.buildRemoveCategoryCommit(categoryId, categoryName, categoryDescription, categoryType);
        commits.put(1L, firstCommit);
        commits.put(2L, secondCommit);
        for (Map.Entry<Long, Commit> commitEntry : commits.entrySet()) {
            dominantService.processCommits(commitEntry.getKey(), commitEntry);
        }
        com.rbkmoney.analytics.domain.db.tables.pojos.Category category = categoryDao.getCategory(64, 2L);

        Assert.assertEquals(categoryId, category.getCategoryId());
        Assert.assertEquals(categoryName, category.getName());
        Assert.assertEquals(categoryDescription, category.getDescription());
        Assert.assertEquals(categoryType.name(), category.getType());
        Assert.assertTrue(category.getDeleted());
    }

}
