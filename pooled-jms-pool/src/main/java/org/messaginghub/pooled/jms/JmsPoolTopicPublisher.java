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

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Topic;
import javax.jms.TopicPublisher;

/**
 * A {@link TopicPublisher} instance that is created and managed by a PooledConnection.
 */
public class JmsPoolTopicPublisher extends JmsPoolMessageProducer implements TopicPublisher, AutoCloseable {

    public JmsPoolTopicPublisher(JmsPoolSession session, TopicPublisher messageProducer, Destination destination, boolean shared) throws JMSException {
        super(session, messageProducer, destination, shared);
    }

    @Override
    public Topic getTopic() throws JMSException {
        return (Topic) getDestination();
    }

    @Override
    public void publish(Message message) throws JMSException {
        super.send(message);
    }

    @Override
    public void publish(Message message, int deliveryMode, int priority, long timeToLive) throws JMSException {
        super.send(message, deliveryMode, priority, timeToLive);
    }

    @Override
    public void publish(Topic topic, Message message) throws JMSException {
        super.send(topic, message);
    }

    @Override
    public void publish(Topic topic, Message message, int deliveryMode, int priority, long timeToLive) throws JMSException {
        super.send(topic, message, deliveryMode, priority, timeToLive);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + " { " + getDelegate() + " }";
    }

    public TopicPublisher getTopicPublisher() throws JMSException {
        return (TopicPublisher) getMessageProducer();
    }
}
