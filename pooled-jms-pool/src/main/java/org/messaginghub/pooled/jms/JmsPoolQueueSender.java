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
import javax.jms.Queue;
import javax.jms.QueueSender;

/**
 * {@link QueueSender} instance that is created and managed by the PooledConnection.
 */
public class JmsPoolQueueSender extends JmsPoolMessageProducer implements QueueSender, AutoCloseable {

    public JmsPoolQueueSender(JmsPoolSession session, QueueSender messageProducer, Destination destination, boolean shared) throws JMSException {
        super(session, messageProducer, destination, shared);
    }

    @Override
    public void send(Queue queue, Message message, int deliveryMode, int priority, long timeToLine) throws JMSException {
        super.send(queue, message, deliveryMode, priority, timeToLine);
    }

    @Override
    public void send(Queue queue, Message message) throws JMSException {
        super.send(queue, message);
    }

    @Override
    public Queue getQueue() throws JMSException {
        return (Queue) getDestination();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + " { " + getDelegate() + " }";
    }

    public QueueSender getQueueSender() throws JMSException {
        return (QueueSender) getMessageProducer();
    }
}
