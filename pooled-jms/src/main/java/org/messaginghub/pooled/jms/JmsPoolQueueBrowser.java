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

import java.util.Enumeration;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueBrowser;

/**
 * A {@link QueueBrowser} which was created by {@link JmsPoolSession}.
 */
public class JmsPoolQueueBrowser implements QueueBrowser, AutoCloseable {

    private final AtomicBoolean closed = new AtomicBoolean();

    private final JmsPoolSession session;
    private final QueueBrowser delegate;

    /**
     * Wraps the QueueBrowser.
     *
     * @param session
     * 		the pooled session that created this object.
     * @param delegate
     * 		the created QueueBrowser to wrap.
     */
    public JmsPoolQueueBrowser(JmsPoolSession session, QueueBrowser delegate) {
        this.session = session;
        this.delegate = delegate;
    }

    @Override
    public Queue getQueue() throws JMSException {
        checkClosed();
        return delegate.getQueue();
    }

    @Override
    public String getMessageSelector() throws JMSException {
        checkClosed();
        return delegate.getMessageSelector();
    }

    @Override
    public Enumeration<?> getEnumeration() throws JMSException {
        checkClosed();
        return delegate.getEnumeration();
    }

    @Override
    public void close() throws JMSException {
        if (closed.compareAndSet(false, true)) {
            // ensure session removes browser from it's list of managed resources.
            session.onQueueBrowserClose(delegate);
            delegate.close();
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + " { " + delegate + " }";
    }

    public QueueBrowser getQueueBrowser() throws JMSException {
        checkClosed();
        return delegate;
    }

    private void checkClosed() throws IllegalStateException {
        if (closed.get()) {
            throw new IllegalStateException("The QueueBrowser is closed");
        }
    }
}
