/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.messaginghub.pooled.jms;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.concurrent.Executors;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TemporaryQueue;
import javax.jms.TextMessage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Timeout(60)
public class PooledConnectionTempQueueTest extends ActiveMQJmsPoolTestSupport {

    private final Logger LOG = LoggerFactory.getLogger(PooledConnectionTempQueueTest.class);

    protected static final String SERVICE_QUEUE = "queue1";

    @Test
    public void testTempQueueIssue() throws JMSException, InterruptedException {
        final JmsPoolConnectionFactory cf = createPooledConnectionFactory();

        try {
            Connection connection = cf.createConnection();
            assertNotNull(connection);
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            LOG.info("First connection was {}", connection);

            // This order seems to matter to reproduce the issue
            connection.close();
            session.close();

            Executors.newSingleThreadExecutor().execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        receiveAndRespondWithMessageIdAsCorrelationId(cf, SERVICE_QUEUE);
                    } catch (JMSException e) {
                        e.printStackTrace();
                    }
                }
            });

            sendWithReplyToTemp(cf, SERVICE_QUEUE);
        } finally {
            cf.stop();
        }
    }

    private void sendWithReplyToTemp(ConnectionFactory cf, String serviceQueue) throws JMSException, InterruptedException {
        Connection connection = cf.createConnection();
        try {
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            TemporaryQueue tempQueue = session.createTemporaryQueue();
            TextMessage msg = session.createTextMessage("Request");
            msg.setJMSReplyTo(tempQueue);
            MessageProducer producer = session.createProducer(session.createQueue(serviceQueue));
            producer.send(msg);

            MessageConsumer consumer = session.createConsumer(tempQueue);
            Message replyMsg = consumer.receive();
            assertNotNull(replyMsg);

            LOG.debug("Reply message: {}", replyMsg);

            consumer.close();

            producer.close();
            session.close();
        } finally {
            connection.close();
        }
    }

    public void receiveAndRespondWithMessageIdAsCorrelationId(ConnectionFactory connectionFactory, String queueName) throws JMSException {
        Connection connection = connectionFactory.createConnection();
        try {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = session.createConsumer(session.createQueue(queueName));
            final javax.jms.Message inMessage = consumer.receive();

            String requestMessageId = inMessage.getJMSMessageID();
            LOG.debug("Received message " + requestMessageId);
            final TextMessage replyMessage = session.createTextMessage("Result");
            replyMessage.setJMSCorrelationID(inMessage.getJMSMessageID());
            final MessageProducer producer = session.createProducer(inMessage.getJMSReplyTo());
            LOG.debug("Sending reply to " + inMessage.getJMSReplyTo());
            producer.send(replyMessage);

            producer.close();
            consumer.close();
            session.close();
        } finally {
            connection.close();
        }
    }
}