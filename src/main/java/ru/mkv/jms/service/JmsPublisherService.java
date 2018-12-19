package ru.mkv.jms.service;

import org.apache.activemq.command.ActiveMQTopic;
import org.springframework.jms.core.JmsTemplate;

/**
 * Created by kirill.marchuk on 19.12.2018
 */
public class JmsPublisherService {

    private final JmsTemplate jmsTemplate;

    public JmsPublisherService(JmsTemplate jmsTemplate) {
        this.jmsTemplate = jmsTemplate;
    }

    public void send(String topicName, String data) {
        jmsTemplate.convertAndSend(new ActiveMQTopic(topicName), data);
    }
}
