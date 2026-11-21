package ru.astrosoup.mineberries.messaging.rabbitmq;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Envelope;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;
import org.springframework.amqp.rabbit.support.DefaultMessagePropertiesConverter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import ru.astrosoup.mineberries.messaging.rabbitmq.RabbitTopologyConfiguration.TopologyProperties;
import tools.jackson.databind.ObjectMapper;

@Configuration(proxyBeanMethods = false)
public class RabbitEventSourceConfiguration {
    @Bean
    RabbitEventSource rabbitEventSource(ObjectMapper mapper, PlatformTransactionManager transactionManager) {
        return new RabbitEventSource(mapper, transactionManager);
    }

    @Bean
    SimpleMessageListenerContainer eventsListenerContainer(ConnectionFactory rabbit, TopologyProperties topology,
                                                          RabbitEventSource eventSource,
                                                          @Value("${mineberries.rabbitmq.events-concurrency:1}") int concurrency) {
        var container = new SimpleMessageListenerContainer(rabbit);
        container.setQueueNames(topology.eventsQueue());
        container.setAcknowledgeMode(AcknowledgeMode.MANUAL);
        container.setConcurrentConsumers(concurrency);
        container.setPrefetchCount(1);
        // The container can log received Messages. Keep the body opaque and omit untrusted headers.
        container.setMessagePropertiesConverter(new DefaultMessagePropertiesConverter() {
            @Override
            public MessageProperties toMessageProperties(AMQP.BasicProperties source, Envelope envelope, String charset) {
                var properties = new MessageProperties();
                if (envelope != null) {
                    properties.setDeliveryTag(envelope.getDeliveryTag());
                    properties.setRedelivered(envelope.isRedeliver());
                }
                return properties;
            }
        });
        container.setMessageListener((ChannelAwareMessageListener) eventSource::receive);
        return container;
    }
}
