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

import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicSession;

import org.junit.Test;
import org.messaginghub.pooled.jms.JmsPoolConnection;
import org.messaginghub.pooled.jms.JmsPoolMessageProducer;
import org.messaginghub.pooled.jms.JmsPoolQueueSender;
import org.messaginghub.pooled.jms.JmsPoolTopicPublisher;

public class JmsPoolWrappedProducersTest extends JmsPoolTestSupport {

    @Test(timeout = 60000)
    public void testCreateMessageProducerWithAnonymousProducerEnabled() throws Exception {
        doTestCreateMessageProducer(true);
    }

    @Test(timeout = 60000)
    public void testCreateMessageProducerWithAnonymousProducerDisabled() throws Exception {
        doTestCreateMessageProducer(false);
    }

    private void doTestCreateMessageProducer(boolean useAnonymousProducers) throws JMSException {
        cf.setUseAnonymousProducers(useAnonymousProducers);

        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        Queue queue1 = session.createTemporaryQueue();
        Queue queue2 = session.createTemporaryQueue();

        JmsPoolMessageProducer producer1 = (JmsPoolMessageProducer) session.createProducer(queue1);
        JmsPoolMessageProducer producer2 = (JmsPoolMessageProducer) session.createProducer(queue2);

        if (useAnonymousProducers) {
            assertSame(producer1.getMessageProducer(), producer2.getMessageProducer());
        } else {
            assertNotSame(producer1.getMessageProducer(), producer2.getMessageProducer());
        }

        connection.close();
    }

    @Test(timeout = 60000)
    public void testCreateTopicPublisherWithAnonymousProducerEnabled() throws Exception {
        doTestCreateTopicPublisher(true);
    }

    @Test(timeout = 60000)
    public void testCreateTopicPublisherWithAnonymousProducerDisabled() throws Exception {
        doTestCreateTopicPublisher(false);
    }

    private void doTestCreateTopicPublisher(boolean useAnonymousProducers) throws JMSException {
        cf.setUseAnonymousProducers(useAnonymousProducers);

        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        TopicSession session = connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);

        Topic topic1 = session.createTopic("Topic-1");
        Topic topic2 = session.createTopic("Topic-2");

        JmsPoolTopicPublisher publisher1 = (JmsPoolTopicPublisher) session.createPublisher(topic1);
        JmsPoolTopicPublisher publisher2 = (JmsPoolTopicPublisher) session.createPublisher(topic2);

        if (useAnonymousProducers) {
            assertSame(publisher1.getMessageProducer(), publisher2.getMessageProducer());
        } else {
            assertNotSame(publisher1.getMessageProducer(), publisher2.getMessageProducer());
        }

        connection.close();
    }

    @Test(timeout = 60000)
    public void testCreateQueueSenderWithAnonymousProducerEnabled() throws Exception {
        doTestCreateQueueSender(true);
    }

    @Test(timeout = 60000)
    public void testCreateQueueSenderWithAnonymousProducerDisabled() throws Exception {
        doTestCreateQueueSender(false);
    }

    private void doTestCreateQueueSender(boolean useAnonymousProducers) throws JMSException {
        cf.setUseAnonymousProducers(useAnonymousProducers);

        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        QueueSession session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);

        Queue queue1 = session.createTemporaryQueue();
        Queue queue2 = session.createTemporaryQueue();

        JmsPoolQueueSender sender1 = (JmsPoolQueueSender) session.createSender(queue1);
        JmsPoolQueueSender sender2 = (JmsPoolQueueSender) session.createSender(queue2);

        if (useAnonymousProducers) {
            assertSame(sender1.getMessageProducer(), sender2.getMessageProducer());
        } else {
            assertNotSame(sender1.getMessageProducer(), sender2.getMessageProducer());
        }

        connection.close();
    }

    @Test(timeout = 60000)
    public void testSendThrowsWhenProducerHasExplicitDestination() throws Exception {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        Queue queue1 = session.createTemporaryQueue();
        Queue queue2 = session.createTemporaryQueue();

        JmsPoolMessageProducer producer = (JmsPoolMessageProducer) session.createProducer(queue1);

        try {
            producer.send(queue2, session.createTextMessage());
            fail("Should only be able to send to queue 1");
        } catch (Exception ex) {
        }

        try {
            producer.send(null, session.createTextMessage());
            fail("Should only be able to send to queue 1");
        } catch (Exception ex) {
        }
    }
}
