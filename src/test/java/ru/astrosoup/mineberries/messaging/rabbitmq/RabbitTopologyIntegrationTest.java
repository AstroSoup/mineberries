package ru.astrosoup.mineberries.messaging.rabbitmq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.amqp.autoconfigure.RabbitAutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import ru.astrosoup.mineberries.integration.AbstractIntegrationTest;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

class RabbitTopologyIntegrationTest extends AbstractIntegrationTest {
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    private final JsonMapper mapper = JsonMapper.builder().build();

    @AfterEach
    void closeClient() {
        http.close();
    }

    @Test
    void declaresTheDefaultTopologyOnStartupAndCanRedeclareIt() {
        var contextRunner = contextRunner();
        for (int startup = 0; startup < 2; startup++) {
            contextRunner.run(context -> {
                assertThat(context).hasNotFailed();
                assertTopology("/", "tasks", "tasks.dlx", "tasks.dlq", 20);
                assertTopology("/", "events", "events.dlx", "events.dlq", 20);
            });
        }
    }

    @Test
    void declaresConfiguredNamesAndDeliveryLimitInTheConfiguredVhost() throws Exception {
        String vhost = "topology-test";
        assertEquals(0, RABBITMQ.execInContainer("rabbitmqctl", "add_vhost", vhost).getExitCode());
        assertEquals(0, RABBITMQ.execInContainer("rabbitmqctl", "set_permissions", "-p", vhost,
                RABBITMQ.getAdminUsername(), ".*", ".*", ".*").getExitCode());

        contextRunner().withPropertyValues(
                "spring.rabbitmq.virtual-host=" + vhost,
                "mineberries.rabbitmq.tasks-queue=worker.in",
                "mineberries.rabbitmq.events-queue=worker.out",
                "mineberries.rabbitmq.tasks-dead-letter-exchange=worker.in.dead",
                "mineberries.rabbitmq.events-dead-letter-exchange=worker.out.dead",
                "mineberries.rabbitmq.tasks-dead-letter-queue=quarantine.in",
                "mineberries.rabbitmq.events-dead-letter-queue=quarantine.out",
                "mineberries.rabbitmq.delivery-limit=3")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertTopology(vhost, "worker.in", "worker.in.dead", "quarantine.in", 3);
                    assertTopology(vhost, "worker.out", "worker.out.dead", "quarantine.out", 3);
                });
    }

    private ApplicationContextRunner contextRunner() {
        return new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(RabbitAutoConfiguration.class))
                .withUserConfiguration(RabbitTopologyConfiguration.class)
                .withPropertyValues(
                        "spring.rabbitmq.host=" + RABBITMQ.getHost(),
                        "spring.rabbitmq.port=" + RABBITMQ.getAmqpPort(),
                        "spring.rabbitmq.username=" + RABBITMQ.getAdminUsername(),
                        "spring.rabbitmq.password=" + RABBITMQ.getAdminPassword());
    }

    private void assertTopology(String vhost, String queue, String exchange, String dlq, int limit) throws Exception {
        String namespace = URLEncoder.encode(vhost, StandardCharsets.UTF_8);
        String queues = "/queues/" + namespace + "/";
        JsonNode source = get(queues + queue);
        assertQueue(source, "quorum");
        JsonNode arguments = source.path("arguments");
        assertEquals("quorum", arguments.path("x-queue-type").asString());
        assertEquals(exchange, arguments.path("x-dead-letter-exchange").asString());
        assertEquals(dlq, arguments.path("x-dead-letter-routing-key").asString());
        assertEquals(limit, arguments.path("x-delivery-limit").asInt());

        JsonNode deadLetters = get(queues + dlq);
        assertQueue(deadLetters, "classic");
        assertEquals("classic", deadLetters.path("arguments").path("x-queue-type").asString());

        JsonNode dlx = get("/exchanges/" + namespace + "/" + exchange);
        assertEquals("direct", dlx.path("type").asString());
        assertTrue(dlx.path("durable").asBoolean());
        assertFalse(dlx.path("auto_delete").asBoolean());

        JsonNode bindings = get("/bindings/" + namespace + "/e/" + exchange + "/q/" + dlq);
        assertEquals(1, bindings.size());
        assertEquals(dlq, bindings.get(0).path("routing_key").asString());
    }

    private static void assertQueue(JsonNode queue, String type) {
        assertEquals(type, queue.path("type").asString());
        assertTrue(queue.path("durable").asBoolean());
        assertFalse(queue.path("exclusive").asBoolean());
        assertFalse(queue.path("auto_delete").asBoolean());
    }

    private JsonNode get(String path) throws Exception {
        String credentials = RABBITMQ.getAdminUsername() + ":" + RABBITMQ.getAdminPassword();
        String authorization = "Basic " + Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
        var request = HttpRequest.newBuilder(URI.create(RABBITMQ.getHttpUrl() + "/api" + path))
                .header("Authorization", authorization)
                .timeout(Duration.ofSeconds(5))
                .build();
        var response = http.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode(), path);
        return mapper.readTree(response.body());
    }
}
