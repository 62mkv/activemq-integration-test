package ru.mkv.jms;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQTopic;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jms.connection.SingleConnectionFactory;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.support.converter.MappingJackson2MessageConverter;
import org.springframework.jms.support.converter.MessageType;
import ru.mkv.jms.service.JmsPublisherService;

import javax.jms.ConnectionFactory;

import static java.lang.Math.random;

/**
 * Created by kirill.marchuk on 19.12.2018
 */
public class JmsPublisherIntTest {

    private static final String BROKER_NAME = "test_broker";
    private static final String TOPIC_NAME = "test_topic";

    private final ConnectionFactory connectionFactory = buildConnectionFactory();
    private final Logger log = LoggerFactory.getLogger(JmsPublisherIntTest.class);
    private BrokerService broker;
    private MappingJackson2MessageConverter converter;

    @Before
    public void setUp() throws Exception {
        log.info("setUp started");
        if (broker == null) {
            broker = buildBroker();
        }
        broker.start();
        converter = new MappingJackson2MessageConverter();
        converter.setTargetType(MessageType.TEXT);
        converter.setTypeIdPropertyName("_type");
        log.info("setUp completed");
    }

    @After
    public void tearDown() throws Exception {
        broker.stop();
    }

    @Test
    public void testPublisherService() {
        final String payload = "test-dummy-message";
        JmsTemplate destinationTemplate = buildJmsTemplate();
        JmsTemplate sourceTemplate = buildJmsTemplate();
        sourceTemplate.setReceiveTimeout(10000);
        JmsPublisherService service = new JmsPublisherService(destinationTemplate);

        log.info("Going to publish message");
        service.send(TOPIC_NAME, payload);
        log.info("Message is published");

        log.info("Going to retrieve message");
        Object retrieved = sourceTemplate.receiveAndConvert(new ActiveMQTopic(TOPIC_NAME));
        log.info("Message is retrieved");

        Assert.assertNotNull(retrieved);
    }

    private BrokerService buildBroker() throws Exception {
        BrokerService broker = new BrokerService();
        broker.setBrokerName(BROKER_NAME);
        broker.setUseShutdownHook(false);
        return broker;
    }

    private ConnectionFactory buildConnectionFactory() {
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory();
        connectionFactory.setBrokerURL(String.format("vm://%s?waitForStart=1000", BROKER_NAME));
        connectionFactory.setPassword("");
        connectionFactory.setUserName("");
        connectionFactory.setClientID("clientId" + random() * 5);
        return new SingleConnectionFactory(connectionFactory);
    }

    private JmsTemplate buildJmsTemplate() {
        JmsTemplate jmsTemplate = new JmsTemplate();
        jmsTemplate.setConnectionFactory(connectionFactory);
        jmsTemplate.setMessageConverter(converter);
        jmsTemplate.setPubSubDomain(true);
        return jmsTemplate;
    }
}
