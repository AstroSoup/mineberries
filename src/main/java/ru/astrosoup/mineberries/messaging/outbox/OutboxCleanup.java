package ru.astrosoup.mineberries.messaging.outbox;

import java.time.Duration;
import java.time.OffsetDateTime;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.convert.DurationStyle;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

/** Deletes expired sent outbox rows, releasing locks after each batch. */
@Component
@Slf4j
public class OutboxCleanup {
    private static final int BATCH_SIZE = 1_000;

    private final JdbcClient jdbc;
    private final TransactionTemplate batchTransaction;
    private final Duration retention;

    public OutboxCleanup(JdbcClient jdbc, PlatformTransactionManager transactionManager,
                         @Value("${mineberries.outbox.cleanup.retention:7d}") String retention) {
        this.jdbc = jdbc;
        this.retention = DurationStyle.detectAndParse(retention);
        if (this.retention.isNegative()) {
            throw new IllegalArgumentException("Outbox cleanup retention must not be negative");
        }
        this.batchTransaction = new TransactionTemplate(transactionManager);
        this.batchTransaction.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
    }

    @Scheduled(cron = "${mineberries.outbox.cleanup.cron:0 0 * * * *}", zone = "UTC")
    public void cleanup() {
        var cutoff = jdbc.sql("SELECT statement_timestamp()").query(OffsetDateTime.class).single().minus(retention);
        long total = 0;
        int deleted;
        do {
            deleted = batchTransaction.execute(status -> jdbc.sql("""
                    DELETE FROM outbox
                    WHERE id IN (
                        SELECT id FROM outbox
                        WHERE sent_at < :cutoff
                        ORDER BY sent_at, id
                        LIMIT :batchSize
                        FOR UPDATE SKIP LOCKED
                    )
                    """)
                    .param("cutoff", cutoff)
                    .param("batchSize", BATCH_SIZE)
                    .update());
            total += deleted;
        } while (deleted == BATCH_SIZE);
        log.info("Outbox cleanup deleted {} rows", total);
    }
}
