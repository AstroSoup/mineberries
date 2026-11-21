package ru.astrosoup.mineberries.messaging.rabbitmq;

import java.time.Duration;

import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.transaction.PlatformTransactionManager;

import ru.astrosoup.mineberries.messaging.rabbitmq.RabbitTopologyConfiguration.TopologyProperties;

@Configuration(proxyBeanMethods = false)
@EnableScheduling
@EnableConfigurationProperties(OutboxRelayConfiguration.RelayProperties.class)
public class OutboxRelayConfiguration {
    @Bean
    OutboxRelay outboxRelay(JdbcClient jdbc, PlatformTransactionManager transactionManager,
                            ConnectionFactory rabbit, TopologyProperties topology, RelayProperties relay) {
        return new OutboxRelay(jdbc, transactionManager, rabbit, topology.tasksQueue(),
                relay.batchSize(), relay.confirmTimeout(), new OutboxRetryBackoff(relay.retryBase(), relay.retryCap()));
    }

    @ConfigurationProperties("mineberries.outbox.relay")
    public record RelayProperties(
            @DefaultValue("100") int batchSize,
            @DefaultValue("5s") Duration confirmTimeout,
            @DefaultValue("1s") Duration retryBase,
            @DefaultValue("1m") Duration retryCap) {}
}
