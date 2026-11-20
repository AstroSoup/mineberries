package ru.astrosoup.mineberries.integration;

import java.sql.Connection;
import java.sql.DriverManager;

import liquibase.Contexts;
import liquibase.Liquibase;
import liquibase.database.jvm.JdbcConnection;
import liquibase.resource.ClassLoaderResourceAccessor;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.rabbitmq.RabbitMQContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Base class for integration tests that need real PostgreSQL and RabbitMQ.
 *
 * <p>The containers are static fields, so they start on the first use and are shared by
 * every test class in the same test JVM. Testcontainers' Ryuk sidecar removes them when
 * the JVM exits. The Liquibase changelog is applied once to the PostgreSQL container
 * before Spring creates its test context.
 *
 * <p>Subclasses can use Spring's normal datasource and RabbitMQ configuration; the
 * container endpoints and credentials are exposed through {@link DynamicPropertySource}.
 */
public abstract class AbstractIntegrationTest {

    private static final String POSTGRES_IMAGE = "postgres:18";
    private static final String RABBITMQ_IMAGE = "rabbitmq:4-management";
    private static final String CHANGELOG = "db/changelog/db.changelog-master.yaml";

    private static final String TEST_DB_NAME = "mineberries_test";
    private static final String TEST_DB_USER = "mineberries_test";
    private static final String TEST_DB_PASSWORD = "mineberries_test";

    private static final String TEST_BROKER_USER = "mineberries_test";
    private static final String TEST_BROKER_PASSWORD = "mineberries_test";

    protected static final PostgreSQLContainer POSTGRES =
            new PostgreSQLContainer(DockerImageName.parse(POSTGRES_IMAGE))
                    .withDatabaseName(TEST_DB_NAME)
                    .withUsername(TEST_DB_USER)
                    .withPassword(TEST_DB_PASSWORD);

    protected static final RabbitMQContainer RABBITMQ =
            new RabbitMQContainer(DockerImageName.parse(RABBITMQ_IMAGE))
                    .withAdminUser(TEST_BROKER_USER)
                    .withAdminPassword(TEST_BROKER_PASSWORD);

    static {
        try {
            Startables.deepStart(POSTGRES, RABBITMQ).join();
        } catch (RuntimeException exception) {
            throw new IllegalStateException("Failed to start the integration test containers", exception);
        }
        applyChangelog();
    }

    private static void applyChangelog() {
        try (Connection connection = DriverManager.getConnection(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
             Liquibase liquibase = new Liquibase(
                     CHANGELOG,
                     new ClassLoaderResourceAccessor(),
                     new JdbcConnection(connection))) {
            liquibase.update(new Contexts("test"));
        } catch (Exception exception) {
            throw new IllegalStateException(
                    "Failed to apply the Liquibase changelog to the integration test database",
                    exception);
        }
    }

    @DynamicPropertySource
    protected static void registerInfrastructureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.datasource.driver-class-name", POSTGRES::getDriverClassName);

        registry.add("spring.rabbitmq.host", RABBITMQ::getHost);
        registry.add("spring.rabbitmq.port", RABBITMQ::getAmqpPort);
        registry.add("spring.rabbitmq.username", RABBITMQ::getAdminUsername);
        registry.add("spring.rabbitmq.password", RABBITMQ::getAdminPassword);
    }
}
