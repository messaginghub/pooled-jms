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

import java.util.concurrent.atomic.AtomicBoolean;

import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;

/**
 * A {@link MessageConsumer} which was created by {@link JmsPoolSession}.
 */
public class JmsPoolMessageConsumer implements MessageConsumer, AutoCloseable {

    private final JmsPoolSession session;
    private final MessageConsumer messageConsumer;
    private final AtomicBoolean closed = new AtomicBoolean();

    /**
     * Wraps the message consumer.
     *
     * @param session
     * 		the pooled session
     * @param messageConsumer
     * 		the created consumer to wrap
     */
    JmsPoolMessageConsumer(JmsPoolSession session, MessageConsumer messageConsumer) {
        this.session = session;
        this.messageConsumer = messageConsumer;
    }

    @Override
    public void close() throws JMSException {
        // ensure session removes consumer from it's list of managed resources.
        if (closed.compareAndSet(false, true)) {
            session.onConsumerClose(messageConsumer);
            messageConsumer.close();
        }
    }

    @Override
    public MessageListener getMessageListener() throws JMSException {
        checkClosed();
        return messageConsumer.getMessageListener();
    }

    @Override
    public String getMessageSelector() throws JMSException {
        checkClosed();
        return messageConsumer.getMessageSelector();
    }

    @Override
    public Message receive() throws JMSException {
        checkClosed();
        return messageConsumer.receive();
    }

    @Override
    public Message receive(long timeout) throws JMSException {
        checkClosed();
        return messageConsumer.receive(timeout);
    }

    @Override
    public Message receiveNoWait() throws JMSException {
        checkClosed();
        return messageConsumer.receiveNoWait();
    }

    @Override
    public void setMessageListener(MessageListener listener) throws JMSException {
        checkClosed();
        messageConsumer.setMessageListener(listener);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + " { " + messageConsumer + " }";
    }

    public MessageConsumer getMessageConsumer() throws JMSException {
        checkClosed();
        return messageConsumer;
    }

    //----- Internal support methods -----------------------------------------//

    protected void checkClosed() throws JMSException {
        if (closed.get()) {
            throw new IllegalStateException("The MessageConsumer is closed");
        }
    }

    protected MessageConsumer getDelegate() {
        return messageConsumer;
    }
}
