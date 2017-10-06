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

import java.util.concurrent.atomic.AtomicBoolean;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;

/**
 * Mock JMS MessageConsumer implementation.
 */
public class MockJMSMessageConsumer implements MessageConsumer, AutoCloseable {

    protected final MockJMSSession session;
    protected final String consumerId;
    protected final MockJMSDestination destination;
    protected final String messageSelector;
    protected final boolean noLocal;

    private final AtomicBoolean closed = new AtomicBoolean();

    private MessageListener messageListener;

    public MockJMSMessageConsumer(MockJMSSession session, String consumerId, MockJMSDestination destination, String messageSelector, boolean noLocal) throws JMSException {
        this.session = session;
        this.consumerId = consumerId;
        this.destination = destination;
        this.messageSelector = messageSelector;
        this.noLocal = noLocal;

        MockJMSConnection connection = session.getConnection();
        connection.getUser().checkCanConsume(destination);

        session.add(this);
    }

    @Override
    public void close() throws JMSException {
        if (closed.compareAndSet(false, true)) {
            session.remove(this);
        }
    }

    void start() {
        // TODO Auto-generated method stub
    }

    //----- Consumer Configuration Methods -----------------------------------//

    @Override
    public String getMessageSelector() throws JMSException {
        checkClosed();
        return messageSelector;
    }

    @Override
    public MessageListener getMessageListener() throws JMSException {
        checkClosed();
        return messageListener;
    }

    @Override
    public void setMessageListener(MessageListener listener) throws JMSException {
        checkClosed();
        this.messageListener = listener;
    }

    //----- Message Receive Methods ------------------------------------------//

    @Override
    public Message receive() throws JMSException {
        checkClosed();
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Message receive(long timeout) throws JMSException {
        checkClosed();
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Message receiveNoWait() throws JMSException {
        checkClosed();
        // TODO Auto-generated method stub
        return null;
    }

    public <T> T receiveBody(Class<T> desired, long timeout) throws JMSException {
        checkClosed();
        //checkMessageListener();
        // TODO Auto-generated method stub
        return null;
    }

    //----- Internal Support Methods -----------------------------------------//

    protected void checkClosed() throws IllegalStateException {
        if (closed.get()) {
            throw new IllegalStateException("The MessageProducer is closed");
        }
    }

    public String getConsumerId() {
        return consumerId;
    }
}
