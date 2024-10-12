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
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.jms.CompletionListener;
import jakarta.jms.Destination;
import jakarta.jms.IllegalStateException;
import jakarta.jms.InvalidDestinationException;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageProducer;

/**
 * A pooled {@link MessageProducer}
 */
public class JmsPoolMessageProducer implements MessageProducer, AutoCloseable {

    private final JmsPoolSession session;
    private final MessageProducer messageProducer;
    private final Destination destination;

    private final AtomicInteger refCount;
    private final boolean anonymousProducer;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private int deliveryMode;
    private boolean disableMessageID;
    private boolean disableMessageTimestamp;
    private int priority;
    private long timeToLive;
    private long deliveryDelay;

    public JmsPoolMessageProducer(JmsPoolSession session, MessageProducer messageProducer, Destination destination, AtomicInteger refCount) throws JMSException {
        this.session = session;
        this.messageProducer = messageProducer;
        this.destination = destination;
        this.refCount = refCount;
        this.anonymousProducer = destination == null;

        this.deliveryMode = messageProducer.getDeliveryMode();
        this.disableMessageID = messageProducer.getDisableMessageID();
        this.disableMessageTimestamp = messageProducer.getDisableMessageTimestamp();
        this.priority = messageProducer.getPriority();
        this.timeToLive = messageProducer.getTimeToLive();

        if (session.isJMSVersionSupported(2, 0)) {
            this.deliveryDelay = messageProducer.getDeliveryDelay();
        }
    }

    @Override
    public void close() throws JMSException {
        doClose(false);
    }

    //----- JMS 1.0 Send Methods ---------------------------------------------//

    @Override
    public void send(Message message) throws JMSException {
        send(message, deliveryMode, priority, timeToLive);
    }

    @Override
    public void send(Message message, int deliveryMode, int priority, long timeToLive) throws JMSException {
        checkClosed();

        if (anonymousProducer) {
            throw new UnsupportedOperationException("Using this method is not supported on producers created without an explicit Destination");
        }

        sendMessage(destination, message, deliveryMode, priority, timeToLive, null);
    }

    @Override
    public void send(Destination destination, Message message) throws JMSException {
        send(destination, message, getDeliveryMode(), getPriority(), getTimeToLive());
    }

    @Override
    public void send(Destination destination, Message message, int deliveryMode, int priority, long timeToLive) throws JMSException {
        checkClosed();
        checkDestinationNotInvalid(destination);

        if (!anonymousProducer) {
            throw new UnsupportedOperationException("Using this method is not supported on producers created with an explicit Destination.");
        }

        sendMessage(destination, message, deliveryMode, priority, timeToLive, null);
    }

    //----- JMS 2.0 Send methods ---------------------------------------------//

    @Override
    public void send(Message message, CompletionListener listener) throws JMSException {
        send(message, deliveryMode, priority, timeToLive, listener);
    }

    @Override
    public void send(Message message, int deliveryMode, int priority, long timeToLive, CompletionListener listener) throws JMSException {
        checkClosed();

        if (anonymousProducer) {
            throw new UnsupportedOperationException("Using this method is not supported on producers created without an explicit Destination");
        }

        if (listener == null) {
            throw new IllegalArgumentException("CompletionListener cannot be null");
        }

        sendMessage(destination, message, deliveryMode, priority, timeToLive, listener);
    }

    @Override
    public void send(Destination destination, Message message, CompletionListener listener) throws JMSException {
        send(destination, message, this.deliveryMode, this.priority, this.timeToLive, listener);
    }

    @Override
    public void send(Destination destination, Message message, int deliveryMode, int priority, long timeToLive, CompletionListener listener) throws JMSException {
        checkClosed();
        checkDestinationNotInvalid(destination);

        if (!anonymousProducer) {
            throw new UnsupportedOperationException("Using this method is not supported on producers created with an explicit Destination.");
        }

        if (listener == null) {
            throw new IllegalArgumentException("CompletionListener cannot be null");
        }

        sendMessage(destination, message, deliveryMode, priority, timeToLive, listener);
    }

    private void sendMessage(Destination destination, Message message, int deliveryMode, int priority, long timeToLive, CompletionListener listener) throws JMSException {
        MessageProducer messageProducer = getMessageProducer();

        // Only one thread can use the producer at a time to allow for dynamic configuration
        // changes to match what's been configured here.
        synchronized (messageProducer) {

            long oldDelayValue = 0;
            if (deliveryDelay != 0 && session.isJMSVersionSupported(2, 0)) {
                oldDelayValue = messageProducer.getDeliveryDelay();
                try {
                    messageProducer.setDeliveryDelay(deliveryDelay);
                } catch (IllegalStateException jmsISE) {
                    // The JMS MessageProducer that backs this wrapper has indicated that it is
                    // likely closed either individually or by the full connection having gone
                    // away.  We will force close it in case only the producer was closed so that
                    // future uses of the producer will result in a new one being opened if the
                    // connection is still live.
                    try {
                        doClose(true);
                    } catch (Exception ex) {
                        // Ignore and throw original failure cause
                    }

                    throw jmsISE;
                }
            }

            // For the non-shared MessageProducer that is also not an anonymous producer we
            // need to call the send method for an explicit MessageProducer otherwise we
            // would be violating the JMS specification in regards to send calls.
            //
            // In all other cases we create an anonymous producer so we call the send with
            // destination parameter version.
            try {
                if (getDelegate().getDestination() != null) {
                    if (listener == null) {
                        messageProducer.send(message, deliveryMode, priority, timeToLive);
                    } else {
                        messageProducer.send(message, deliveryMode, priority, timeToLive, listener);
                    }
                } else {
                    if (listener == null) {
                        messageProducer.send(destination, message, deliveryMode, priority, timeToLive);
                    } else {
                        messageProducer.send(destination, message, deliveryMode, priority, timeToLive, listener);
                    }
                }
            } catch (IllegalStateException jmsISE) {
                // The JMS MessageProducer that backs this wrapper has indicated that it is
                // likely closed either individually or by the full connection having gone
                // away.  We will force close it in case only the producer was closed so that
                // future uses of the producer will result in a new one being opened if the
                // connection is still live.
                try {
                    doClose(true);
                } catch (Exception ex) {
                    // Ignore and throw original failure cause
                }

                throw jmsISE;
            } finally {
                if (!closed.get() && deliveryDelay != 0 && session.isJMSVersionSupported(2, 0)) {
                    try {
                        messageProducer.setDeliveryDelay(oldDelayValue);
                    } catch (IllegalStateException jmsISE) {
                        try {
                            doClose(true);
                        } catch (Exception ex) {
                            // Ignore and throw original failure cause
                        }

                        throw jmsISE;
                    }
                }
            }
        }
    }

    //----- MessageProducer configuration ------------------------------------//

    @Override
    public Destination getDestination() throws JMSException {
        checkClosed();
        return destination;
    }

    @Override
    public int getDeliveryMode() throws JMSException {
        checkClosed();
        return deliveryMode;
    }

    @Override
    public void setDeliveryMode(int deliveryMode) throws JMSException {
        checkClosed();
        this.deliveryMode = deliveryMode;
    }

    @Override
    public boolean getDisableMessageID() throws JMSException {
        checkClosed();
        return disableMessageID;
    }

    @Override
    public void setDisableMessageID(boolean disableMessageID) throws JMSException {
        checkClosed();
        this.disableMessageID = disableMessageID;
    }

    @Override
    public boolean getDisableMessageTimestamp() throws JMSException {
        checkClosed();
        return disableMessageTimestamp;
    }

    @Override
    public void setDisableMessageTimestamp(boolean disableMessageTimestamp) throws JMSException {
        checkClosed();
        this.disableMessageTimestamp = disableMessageTimestamp;
    }

    @Override
    public int getPriority() throws JMSException {
        checkClosed();
        return priority;
    }

    @Override
    public void setPriority(int priority) throws JMSException {
        checkClosed();
        this.priority = priority;
    }

    @Override
    public long getTimeToLive() throws JMSException {
        checkClosed();
        return timeToLive;
    }

    @Override
    public void setTimeToLive(long timeToLive) throws JMSException {
        checkClosed();
        this.timeToLive = timeToLive;
    }

    @Override
    public long getDeliveryDelay() throws JMSException {
        checkClosed();
        session.checkClientJMSVersionSupport(2, 0);
        return deliveryDelay;
    }

    @Override
    public void setDeliveryDelay(long deliveryDelay) throws JMSException {
        checkClosed();
        session.checkClientJMSVersionSupport(2, 0);

        this.deliveryDelay = deliveryDelay;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + " { " + messageProducer + " }";
    }

    public MessageProducer getMessageProducer() throws JMSException {
        checkClosed();
        return messageProducer;
    }

    //----- Internal Implementation ------------------------------------------//

    /**
     * @return is this {@link MessageProducer} wrapper an anonymous variant.
     */
    public boolean isAnonymousProducer() {
        return this.anonymousProducer;
    }

    /**
     * @return the reference counter used to manage this wrapper's lifetime.
     */
    public AtomicInteger getRefCount() {
        return this.refCount;
    }

    /**
     * @return the underlying {@link MessageProducer} that this wrapper object is a proxy to.
     */
    public MessageProducer getDelegate() {
        return messageProducer;
    }

    /**
     * @return the underlying Destination that this wrapper object applies to the delegate {@link MessageProducer}.
     */
    public Destination getDelegateDestination() {
        return destination;
    }

    private void doClose(boolean force) throws JMSException {
        if (closed.compareAndSet(false, true)) {
            session.onMessageProducerClosed(this, force);
        }
    }

    protected void checkClosed() throws IllegalStateException {
        if (closed.get()) {
            throw new IllegalStateException("This message producer has been closed.");
        }
    }

    private void checkDestinationNotInvalid(Destination destination) throws InvalidDestinationException {
        if (destination == null) {
            throw new InvalidDestinationException("Destination must not be null");
        }
    }
}
