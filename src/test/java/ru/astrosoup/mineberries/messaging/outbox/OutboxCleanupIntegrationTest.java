package ru.astrosoup.mineberries.messaging.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.sql.Types;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.scheduling.annotation.SchedulingConfiguration;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionExecution;
import org.springframework.transaction.TransactionExecutionListener;

import ru.astrosoup.mineberries.integration.AbstractIntegrationTest;

class OutboxCleanupIntegrationTest extends AbstractIntegrationTest {
    private final String type = "cleanup-test." + UUID.randomUUID();
    private JdbcClient jdbc;
    private DataSourceTransactionManager transactions;

    @BeforeEach
    void setUp() {
        var source = dataSource();
        jdbc = JdbcClient.create(source);
        transactions = new DataSourceTransactionManager(source);
    }

    @AfterEach
    void deleteTestRows() {
        jdbc.sql("DELETE FROM outbox WHERE type = ?").param(type).update();
    }

    @Test
    void deletesOnlyExpiredSentRows() {
        insertRows(1, 8);
        var recent = insertRows(1, 1);
        var unsent = insertRows(2, null);

        new OutboxCleanup(jdbc, transactions, "7d").cleanup();

        assertThat(remainingIds()).containsExactlyInAnyOrder(recent.getFirst(), unsent.get(0), unsent.get(1));
    }

    @Test
    void commitsEachBatchBeforeDeletingTheNext() {
        insertRows(2503, 8);
        var observer = JdbcClient.create(dataSource());
        var remainingAfterCommit = new ArrayList<Long>();
        transactions.addListener(new TransactionExecutionListener() {
            @Override
            public void afterCommit(TransactionExecution transaction, Throwable failure) {
                assertNull(failure);
                remainingAfterCommit.add(observer.sql("SELECT count(*) FROM outbox WHERE type = ?")
                        .param(type).query(Long.class).single());
            }
        });

        new OutboxCleanup(jdbc, transactions, "7d").cleanup();

        assertEquals(List.of(1503L, 503L, 0L), remainingAfterCommit);
    }

    @Test
    void scheduledCleanupUsesConfiguredCronAndRetention() {
        insertRows(1, 4);
        var kept = insertRows(1, 2);

        new ApplicationContextRunner()
                .withUserConfiguration(OutboxCleanup.class, SchedulingConfiguration.class)
                .withBean(JdbcClient.class, () -> jdbc)
                .withBean(PlatformTransactionManager.class, () -> transactions)
                .withPropertyValues("mineberries.outbox.cleanup.cron=*/1 * * * * *",
                        "mineberries.outbox.cleanup.retention=3d")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    await().atMost(Duration.ofSeconds(5)).until(() -> remainingIds().equals(kept));
                });
    }

    private List<UUID> insertRows(int count, Integer sentDaysAgo) {
        return jdbc.sql("""
                INSERT INTO outbox (message_id, kind, type, payload, created_at, sent_at)
                SELECT gen_random_uuid(), 'task', :type, '{}'::jsonb,
                       statement_timestamp() - INTERVAL '20 days',
                       statement_timestamp() - :days * INTERVAL '1 day'
                FROM generate_series(1, :count)
                RETURNING message_id
                """)
                .param("type", type)
                .param("days", sentDaysAgo, Types.INTEGER)
                .param("count", count)
                .query(UUID.class)
                .list();
    }

    private List<UUID> remainingIds() {
        return jdbc.sql("SELECT message_id FROM outbox WHERE type = ?").param(type).query(UUID.class).list();
    }

    private static DriverManagerDataSource dataSource() {
        return new DriverManagerDataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
    }
}
