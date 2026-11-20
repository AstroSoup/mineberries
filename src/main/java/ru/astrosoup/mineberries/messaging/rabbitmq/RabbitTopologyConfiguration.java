package ru.astrosoup.mineberries.messaging.rabbitmq;

import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Declarables;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Declares the shared task and event topology on the configured RabbitMQ vhost. */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(RabbitTopologyConfiguration.TopologyProperties.class)
public class RabbitTopologyConfiguration {

    @Bean
    Declarables tasksTopology(TopologyProperties properties) {
        return topology(properties.tasksQueue(), properties.tasksDeadLetterExchange(),
                properties.tasksDeadLetterQueue(), properties.deliveryLimit());
    }

    @Bean
    Declarables eventsTopology(TopologyProperties properties) {
        return topology(properties.eventsQueue(), properties.eventsDeadLetterExchange(),
                properties.eventsDeadLetterQueue(), properties.deliveryLimit());
    }

    @Bean
    SmartInitializingSingleton declareRabbitTopology(AmqpAdmin admin) {
        // Without this, RabbitAdmin normally waits for the first AMQP connection.
        return admin::initialize;
    }

    private static Declarables topology(String queueName, String exchangeName, String deadLetterQueueName,
                                        int deliveryLimit) {
        var deadLetterExchange = new DirectExchange(exchangeName, true, false);
        var deadLetterQueue = QueueBuilder.durable(deadLetterQueueName).classic().build();
        var queue = QueueBuilder.durable(queueName)
                .quorum()
                .deadLetterExchange(exchangeName)
                .deadLetterRoutingKey(deadLetterQueueName)
                .deliveryLimit(deliveryLimit)
                .build();
        return new Declarables(deadLetterExchange, deadLetterQueue, queue,
                BindingBuilder.bind(deadLetterQueue).to(deadLetterExchange).with(deadLetterQueueName));
    }

    @ConfigurationProperties("mineberries.rabbitmq")
    public record TopologyProperties(
            @DefaultValue("tasks") String tasksQueue,
            @DefaultValue("events") String eventsQueue,
            @DefaultValue("tasks.dlx") String tasksDeadLetterExchange,
            @DefaultValue("events.dlx") String eventsDeadLetterExchange,
            @DefaultValue("tasks.dlq") String tasksDeadLetterQueue,
            @DefaultValue("events.dlq") String eventsDeadLetterQueue,
            @DefaultValue("20") int deliveryLimit) {

        public TopologyProperties {
            if (deliveryLimit < 1) {
                throw new IllegalArgumentException("mineberries.rabbitmq.delivery-limit must be positive");
            }
        }
    }
}
