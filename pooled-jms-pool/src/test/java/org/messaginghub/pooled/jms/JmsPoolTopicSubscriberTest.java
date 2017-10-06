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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;

import org.junit.Test;
import org.messaginghub.pooled.jms.JmsPoolConnection;
import org.messaginghub.pooled.jms.JmsPoolTopicSubscriber;
import org.messaginghub.pooled.jms.mock.MockJMSTopicSubscriber;

/**
 * Tests for the pool JMS TopicSubscriber wrapper.
 */
public class JmsPoolTopicSubscriberTest extends JmsPoolTestSupport {

    @Test
    public void testToString() throws JMSException {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createTopicConnection();
        TopicSession session = connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = session.createTemporaryTopic();
        TopicSubscriber subscriber = session.createSubscriber(topic);

        assertNotNull(subscriber.toString());
    }

    @Test
    public void testGetTopic() throws JMSException {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createTopicConnection();
        TopicSession session = connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = session.createTemporaryTopic();
        TopicSubscriber subscriber = session.createSubscriber(topic);

        assertNotNull(subscriber.getTopic());
        assertSame(topic, subscriber.getTopic());

        subscriber.close();

        try {
            subscriber.getTopic();
            fail("Cannot read topic on closed subscriber");
        } catch (IllegalStateException ise) {}
    }

    @Test
    public void testGetNoLocal() throws JMSException {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createTopicConnection();
        TopicSession session = connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = session.createTemporaryTopic();
        TopicSubscriber subscriber = session.createDurableSubscriber(topic, "name", "color = red", true);

        assertTrue(subscriber.getNoLocal());

        subscriber.close();

        try {
            subscriber.getNoLocal();
            fail("Cannot read state on closed subscriber");
        } catch (IllegalStateException ise) {}
    }

    @Test
    public void testGetTopicSubscriber() throws JMSException {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createTopicConnection();
        TopicSession session = connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = session.createTemporaryTopic();
        JmsPoolTopicSubscriber subscriber = (JmsPoolTopicSubscriber) session.createDurableSubscriber(topic, "name", "color = red", true);

        assertNotNull(subscriber.getTopicSubscriber());
        assertTrue(subscriber.getTopicSubscriber() instanceof MockJMSTopicSubscriber);

        subscriber.close();

        try {
            subscriber.getTopicSubscriber();
            fail("Cannot read state on closed subscriber");
        } catch (IllegalStateException ise) {}
    }
}
