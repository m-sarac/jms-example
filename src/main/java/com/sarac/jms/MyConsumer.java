package com.sarac.jms;

import javax.annotation.PreDestroy;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;
import javax.jms.ConnectionFactory;
import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.quarkus.runtime.StartupEvent;

@ApplicationScoped
public class MyConsumer implements MessageListener {

	private static final Logger logger = LoggerFactory.getLogger(MyConsumer.class);
	
	JMSContext context;
	@Inject 
	ConnectionFactory connectionFactory;

	public void init(@Observes StartupEvent event) {
		try {
			context = connectionFactory.createContext(JMSContext.AUTO_ACKNOWLEDGE);
			Topic topic = context.createTopic("my-topic");			
			JMSConsumer consumer = context.createSharedDurableConsumer(topic, "sarac-test");
			consumer.setMessageListener(this);
		} catch (Exception e) {
			throw new IllegalStateException("Failed to start consumer for my-topic", e);
		}
		logger.info("Started listening topic {}", "my-topic");
	}

	/**
	 * @see MessageListener#onMessage(Message)
	 */
	public void onMessage(Message message) {
		if (message == null) {
			return;
		}
		try {
			if (message instanceof TextMessage) {
				logger.info(message.getBody(String.class));
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@PreDestroy
	public void shutdown() {
		if (context != null) {
			context.close();
		}
	}

}
