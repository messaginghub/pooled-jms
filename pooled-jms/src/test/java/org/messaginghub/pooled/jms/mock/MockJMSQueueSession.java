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
package org.messaginghub.pooled.jms.mock;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.QueueSession;
import javax.jms.TemporaryTopic;
import javax.jms.Topic;
import javax.jms.TopicPublisher;
import javax.jms.TopicSubscriber;

/**
 * Mock JMS Queue Session
 */
public class MockJMSQueueSession extends MockJMSSession implements QueueSession, AutoCloseable {

    public MockJMSQueueSession(String sessionId, int sessionMode, MockJMSConnection connection) {
        super(sessionId, sessionMode, connection);
    }

    @Override
    public MessageConsumer createConsumer(Destination destination) throws JMSException {
        if (destination instanceof Topic) {
            throw new IllegalStateException("Operation not supported by a QueueSession");
        }
        return super.createConsumer(destination);
    }

    @Override
    public MessageConsumer createConsumer(Destination destination, String messageSelector) throws JMSException {
        if (destination instanceof Topic) {
            throw new IllegalStateException("Operation not supported by a QueueSession");
        }
        return super.createConsumer(destination, messageSelector);
    }

    @Override
    public MessageConsumer createConsumer(Destination destination, String messageSelector, boolean noLocal) throws JMSException {
        if (destination instanceof Topic) {
            throw new IllegalStateException("Operation not supported by a QueueSession");
        }
        return super.createConsumer(destination, messageSelector, noLocal);
    }

    @Override
    public TopicSubscriber createDurableSubscriber(Topic topic, String name) throws JMSException {
        throw new IllegalStateException("Operation not supported by a QueueSession");
    }

    @Override
    public TopicSubscriber createDurableSubscriber(Topic topic, String name, String messageSelector, boolean noLocal) throws JMSException {
        throw new IllegalStateException("Operation not supported by a QueueSession");
    }

    @Override
    public MessageProducer createProducer(Destination destination) throws JMSException {
        if (destination instanceof Topic) {
            throw new IllegalStateException("Operation not supported by a QueueSession");
        }
        return super.createProducer(destination);
    }

    @Override
    public TemporaryTopic createTemporaryTopic() throws JMSException {
        throw new IllegalStateException("Operation not supported by a QueueSession");
    }

    @Override
    public Topic createTopic(String topicName) throws JMSException {
        throw new IllegalStateException("Operation not supported by a QueueSession");
    }

    @Override
    public void unsubscribe(String name) throws JMSException {
        throw new IllegalStateException("Operation not supported by a QueueSession");
    }

    @Override
    public TopicPublisher createPublisher(Topic topic) throws JMSException {
        throw new IllegalStateException("Operation not supported by a QueueSession");
    }

    @Override
    public TopicSubscriber createSubscriber(Topic topic) throws JMSException {
        throw new IllegalStateException("Operation not supported by a QueueSession");
    }

    @Override
    public TopicSubscriber createSubscriber(Topic topic, String messageSelector, boolean noLocal) throws JMSException {
        throw new IllegalStateException("Operation not supported by a QueueSession");
    }

    @Override
    public MessageConsumer createSharedConsumer(Topic topic, String name) throws JMSException {
        throw new IllegalStateException("Operation not supported by a QueueSession");
    }

    @Override
    public MessageConsumer createSharedConsumer(Topic topic, String name, String selector) throws JMSException {
        throw new IllegalStateException("Operation not supported by a QueueSession");
    }

    @Override
    public MessageConsumer createSharedDurableConsumer(Topic topic, String name) throws JMSException {
        throw new IllegalStateException("Operation not supported by a QueueSession");
    }

    @Override
    public MessageConsumer createSharedDurableConsumer(Topic topic, String name, String selector) throws JMSException {
        throw new IllegalStateException("Operation not supported by a QueueSession");
    }
}
