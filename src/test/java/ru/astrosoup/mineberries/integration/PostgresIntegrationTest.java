package ru.astrosoup.mineberries.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.jdbc.test.autoconfigure.AutoConfigureTestDatabase;
import org.springframework.boot.jdbc.test.autoconfigure.JdbcTest;
import org.springframework.jdbc.core.JdbcTemplate;

@JdbcTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
class PostgresIntegrationTest extends AbstractIntegrationTest {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    void insertsAndReadsRow() {
        String nickname = "it_" + UUID.randomUUID().toString().replace("-", "").substring(0, 8);

        int inserted = jdbcTemplate.update(
                "INSERT INTO app_user (nickname, password_hash) VALUES (?, ?)",
                nickname,
                "test-password-hash");
        assertEquals(1, inserted);

        String passwordHash = jdbcTemplate.queryForObject(
                "SELECT password_hash FROM app_user WHERE nickname = ?",
                String.class,
                nickname);
        assertEquals("test-password-hash", passwordHash);
    }
}
