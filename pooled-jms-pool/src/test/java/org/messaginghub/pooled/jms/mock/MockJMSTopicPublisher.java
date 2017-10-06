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

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Topic;
import javax.jms.TopicPublisher;

/**
 * Mock JMS TopicPublisher implementation.
 */
public class MockJMSTopicPublisher extends MockJMSMessageProducer implements AutoCloseable, TopicPublisher {

    public MockJMSTopicPublisher(MockJMSSession session, String producerId, MockJMSDestination destination) throws JMSException {
        super(session, producerId, destination);
    }

    @Override
    public Topic getTopic() throws IllegalStateException {
        checkClosed();
        return (Topic) this.destination;
    }

    @Override
    public void publish(Message message) throws JMSException {
        super.send(message);
    }

    @Override
    public void publish(Topic topic, Message message) throws JMSException {
        super.send(topic, message);
    }

    @Override
    public void publish(Message message, int deliveryMode, int priority, long timeToLive) throws JMSException {
        super.send(message, deliveryMode, priority, timeToLive);
    }

    @Override
    public void publish(Topic topic, Message message, int deliveryMode, int priority, long timeToLive) throws JMSException {
        super.send(topic, message, deliveryMode, priority, timeToLive);
    }
}
