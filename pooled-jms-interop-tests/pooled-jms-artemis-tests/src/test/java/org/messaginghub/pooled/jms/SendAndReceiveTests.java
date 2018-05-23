package org.messaginghub.pooled.jms;

import org.apache.activemq.artemis.jms.client.ActiveMQQueue;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.junit.Assert;
import org.junit.Test;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.util.concurrent.atomic.AtomicInteger;

public class SendAndReceiveTests extends ArtemisJmsPoolTestSupport {

    @Test
    public void testSendRecvRecvSendRecvWithCore() throws Exception {
        final ConnectionFactory connectionFactory = getCoreConnectionFactory();
        doTestSendRecvRecvSendRecv(connectionFactory);
    }

    @Test
    public void testSendRecvRecvSendRecvWithAMQP() throws Exception {
        final ConnectionFactory connectionFactory = getAMQPConnectionFactory();
        doTestSendRecvRecvSendRecv(connectionFactory);
    }

    void doTestSendRecvRecvSendRecv(final ConnectionFactory connectionFactory) throws Exception {
        final AtomicInteger counter = new AtomicInteger(0);
        final Queue queue = new ActiveMQQueue("MyLiQueueRe");

        // send
        SendAndReceiveTests.sendMessage(connectionFactory, counter, queue);

        // receive something
        try (
                Connection connection = connectionFactory.createConnection();
                Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                MessageConsumer consumer = session.createConsumer(queue);
        ) {
            connection.start();
            final TextMessage message = (TextMessage) consumer.receive(2000);
            Assert.assertNotNull(message);
        }
        // receive nothing
        try (
                Connection connection = connectionFactory.createConnection();
                Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                MessageConsumer consumer = session.createConsumer(queue);
        ) {
            connection.start();
            final TextMessage message = (TextMessage) consumer.receive(2000);
            Assert.assertNull(message);
        }

        // send
        SendAndReceiveTests.sendMessage(connectionFactory, counter, queue);

        // tried this just in case the cause is similar to https://issues.jboss.org/browse/ENTMQBR-72
//        Thread.sleep(120000);

        // receive something
        try (
                Connection connection = connectionFactory.createConnection();
                Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                MessageConsumer consumer = session.createConsumer(queue);
        ) {
            connection.start();
            final TextMessage message = (TextMessage) consumer.receive(2000);
            Assert.assertNotNull(message);
            System.out.println(message.getText());
        }
    }

    static void sendMessage(ConnectionFactory connectionFactory, AtomicInteger counter, Queue queue) throws
            JMSException {
        try (
                Connection connection = connectionFactory.createConnection();
                Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                MessageProducer producer = session.createProducer(queue);
        ) {
            connection.start();
            final TextMessage message = session.createTextMessage();
            message.setText(String.valueOf(counter.getAndIncrement()));
            producer.send(message);
        }
    }

    ConnectionFactory getCoreConnectionFactory() {
        return cf;
    }

    ConnectionFactory getAMQPConnectionFactory() {
        JmsConnectionFactory connectionFactory = new JmsConnectionFactory();
        connectionFactory.setRemoteURI("failover:(amqp://localhost:61616)");
        connectionFactory.setUsername("admin");
        connectionFactory.setPassword("admin");

        JmsPoolConnectionFactory pool = new org.messaginghub.pooled.jms.JmsPoolConnectionFactory();
        pool.setConnectionFactory(connectionFactory);
        pool.setUseAnonymousProducers(false);
        return pool;
    }
}
