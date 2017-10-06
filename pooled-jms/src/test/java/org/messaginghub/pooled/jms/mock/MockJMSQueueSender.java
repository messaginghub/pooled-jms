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

import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Queue;
import javax.jms.QueueSender;

/**
 * Mock JMS QueueSender implementation.
 */
public class MockJMSQueueSender extends MockJMSMessageProducer implements QueueSender, AutoCloseable {

    public MockJMSQueueSender(MockJMSSession session, String producerId, MockJMSDestination destination) throws JMSException {
        super(session, producerId, destination);
    }

    @Override
    public Queue getQueue() throws IllegalStateException {
        checkClosed();
        return (Queue) this.destination;
    }

    @Override
    public void send(Queue queue, Message message) throws JMSException {
        super.send(queue, message);
    }

    @Override
    public void send(Queue queue, Message message, int deliveryMode, int priority, long timeToLive) throws JMSException {
        super.send(queue, message, deliveryMode, priority, timeToLive);
    }
}
