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
import javax.jms.Queue;
import javax.jms.QueueReceiver;

/**
 * Mock Implementation of a JMS QueueReceiver
 */
public class MockJMSQueueReceiver extends MockJMSMessageConsumer implements AutoCloseable, QueueReceiver {

    protected MockJMSQueueReceiver(MockJMSSession session, String consumerId, MockJMSDestination destination, String messageSelector) throws JMSException {
        super(session, consumerId, destination, messageSelector, false);
    }

    @Override
    public Queue getQueue() throws IllegalStateException {
        checkClosed();
        return (Queue) this.destination;
    }
}
