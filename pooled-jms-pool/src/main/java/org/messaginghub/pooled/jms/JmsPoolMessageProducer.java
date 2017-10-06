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

import javax.jms.CompletionListener;
import javax.jms.Destination;
import javax.jms.IllegalStateException;
import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;

/**
 * A pooled {@link MessageProducer}
 */
public class JmsPoolMessageProducer implements MessageProducer, AutoCloseable {

    private final JmsPoolSession session;
    private final MessageProducer messageProducer;
    private final Destination destination;

    private final boolean shared;
    private final boolean anonymousProducer;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private int deliveryMode;
    private boolean disableMessageID;
    private boolean disableMessageTimestamp;
    private int priority;
    private long timeToLive;
    private long deliveryDelay;

    public JmsPoolMessageProducer(JmsPoolSession session, MessageProducer messageProducer, Destination destination, boolean shared) throws JMSException {
        this.session = session;
        this.messageProducer = messageProducer;
        this.destination = destination;
        this.shared = shared;
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
        if (closed.compareAndSet(false, true)) {
            session.onMessageProducerClosed(this);
            if (!shared) {
                this.messageProducer.close();
            }
        }
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
            throw new IllegalArgumentException("JmsCompletetionListener cannot be null");
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
            throw new IllegalArgumentException("JmsCompletetionListener cannot be null");
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
                messageProducer.setDeliveryDelay(deliveryDelay);
            }

            // For the non-shared MessageProducer that is also not an anonymous producer we
            // need to call the send method for an explicit MessageProducer otherwise we
            // would be violating the JMS specification in regards to send calls.
            //
            // In all other cases we create an anonymous producer so we call the send with
            // destination parameter version.
            try {
                if (!shared && !anonymousProducer) {
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
            } finally {
                if (deliveryDelay != 0 && session.isJMSVersionSupported(2, 0)) {
                    messageProducer.setDeliveryDelay(oldDelayValue);
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
        this.messageProducer.setDeliveryDelay(deliveryDelay);
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

    protected MessageProducer getDelegate() {
        return messageProducer;
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
