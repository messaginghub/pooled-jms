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

import java.util.Collections;
import java.util.Enumeration;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Queue;
import javax.jms.QueueBrowser;

/**
 * Mock JMS QueueBrowser implementation.
 */
public class MockJMSQueueBrowser implements QueueBrowser, AutoCloseable, Enumeration<Message> {

    private final AtomicBoolean closed = new AtomicBoolean();

    @SuppressWarnings("unused")
    private final MockJMSSession session;
    private final String browserId;
    private final String messageSelector;
    private final MockJMSDestination queue;

    public MockJMSQueueBrowser(MockJMSSession session, String browserId, MockJMSDestination queue, String messageSelector) {
        this.session = session;
        this.browserId = browserId;
        this.queue = queue;
        this.messageSelector = messageSelector;
    }

    @Override
    public void close() throws JMSException {
        if (closed.compareAndSet(false, true)) {

        }
    }

    @Override
    public Enumeration<?> getEnumeration() throws JMSException {
        checkClosed();
        return Collections.emptyEnumeration();
    }

    @Override
    public boolean hasMoreElements() {
        return false;
    }

    @Override
    public Message nextElement() {
        throw new NoSuchElementException();
    }

    //----- Browser Configuration --------------------------------------------//

    @Override
    public Queue getQueue() throws JMSException {
        checkClosed();
        return (Queue) queue;
    }

    @Override
    public String getMessageSelector() throws JMSException {
        checkClosed();
        return messageSelector;
    }

    public String getBrowserId() {
        return browserId;
    }

    //----- Internal Support Methods -----------------------------------------//

    protected void checkClosed() throws IllegalStateException {
        if (closed.get()) {
            throw new IllegalStateException("The QueueBrowser is closed");
        }
    }
}
