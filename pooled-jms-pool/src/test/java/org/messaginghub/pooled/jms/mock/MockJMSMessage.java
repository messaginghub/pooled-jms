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

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotReadableException;
import javax.jms.MessageNotWriteableException;

/**
 * Mock JMS Message Implementation
 */
public class MockJMSMessage implements Message {

    protected Map<String, Object> properties = new HashMap<String, Object>();

    protected int priority = javax.jms.Message.DEFAULT_PRIORITY;
    protected String groupId;
    protected int groupSequence;
    protected String messageId;
    protected long expiration;
    protected long deliveryTime;
    protected boolean deliveryTimeTransmitted;
    protected long timestamp;
    protected String correlationId;
    protected boolean persistent = true;
    protected int redeliveryCount;
    protected String type;
    protected MockJMSDestination destination;
    protected MockJMSDestination replyTo;
    protected String userId;

    protected boolean readOnly;
    protected boolean readOnlyBody;
    protected boolean readOnlyProperties;

    @Override
    public String getJMSMessageID() throws JMSException {
        return messageId.toString();
    }

    @Override
    public void setJMSMessageID(String messageId) throws JMSException {
        this.messageId = messageId;
    }

    @Override
    public long getJMSTimestamp() throws JMSException {
        return timestamp;
    }

    @Override
    public void setJMSTimestamp(long timestamp) throws JMSException {
        this.timestamp = timestamp;
    }

    @Override
    public byte[] getJMSCorrelationIDAsBytes() throws JMSException {
        return correlationId.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public void setJMSCorrelationIDAsBytes(byte[] correlationID) throws JMSException {
        correlationId = new String(correlationID, StandardCharsets.UTF_8);
    }

    @Override
    public void setJMSCorrelationID(String correlationID) throws JMSException {
        this.correlationId = correlationID;
    }

    @Override
    public String getJMSCorrelationID() throws JMSException {
        return correlationId;
    }

    @Override
    public Destination getJMSReplyTo() throws JMSException {
        return replyTo;
    }

    @Override
    public void setJMSReplyTo(Destination replyTo) throws JMSException {
        this.replyTo = (MockJMSDestination) replyTo;
    }

    @Override
    public Destination getJMSDestination() throws JMSException {
        return destination;
    }

    @Override
    public void setJMSDestination(Destination destination) throws JMSException {
        this.destination = (MockJMSDestination) destination;
    }

    @Override
    public int getJMSDeliveryMode() throws JMSException {
        return persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT;
    }

    @Override
    public void setJMSDeliveryMode(int deliveryMode) throws JMSException {
        switch (deliveryMode) {
            case DeliveryMode.PERSISTENT:
                persistent = true;
                break;
            case DeliveryMode.NON_PERSISTENT:
                persistent = false;
                break;
            default:
                throw new JMSException(String.format("Invalid DeliveryMode specific: %d", deliveryMode));
        }
    }

    @Override
    public boolean getJMSRedelivered() throws JMSException {
        return redeliveryCount > 0;
    }

    @Override
    public void setJMSRedelivered(boolean redelivered) throws JMSException {
        if (redelivered) {
            redeliveryCount = 1;
        } else {
            redeliveryCount = 0;
        }
    }

    @Override
    public String getJMSType() throws JMSException {
        return type;
    }

    @Override
    public void setJMSType(String type) throws JMSException {
        this.type = type;
    }

    @Override
    public long getJMSExpiration() throws JMSException {
        return expiration;
    }

    @Override
    public void setJMSExpiration(long expiration) throws JMSException {
        this.expiration = expiration;
    }

    @Override
    public int getJMSPriority() throws JMSException {
        return priority;
    }

    @Override
    public void setJMSPriority(int priority) throws JMSException {
        if (priority < 0 || priority > 9) {
            throw new JMSException(String.format("Priority value given {%d} is out of range (0..9)", priority));
        }

        this.priority = priority;
    }

    @Override
    public long getJMSDeliveryTime() throws JMSException {
        return deliveryTime;
    }

    @Override
    public void setJMSDeliveryTime(long deliveryTime) throws JMSException {
        this.deliveryTime = deliveryTime;
    }

    @Override
    public void clearProperties() throws JMSException {
        properties.clear();
    }

    @Override
    public boolean propertyExists(String name) throws JMSException {
        return properties.containsKey(name);
    }

    @Override
    public boolean getBooleanProperty(String name) throws JMSException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public byte getByteProperty(String name) throws JMSException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public short getShortProperty(String name) throws JMSException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getIntProperty(String name) throws JMSException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public long getLongProperty(String name) throws JMSException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public float getFloatProperty(String name) throws JMSException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public double getDoubleProperty(String name) throws JMSException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public String getStringProperty(String name) throws JMSException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Object getObjectProperty(String name) throws JMSException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Enumeration<?> getPropertyNames() throws JMSException {
        return Collections.enumeration(properties.keySet());
    }

    @Override
    public void setBooleanProperty(String name, boolean value) throws JMSException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setByteProperty(String name, byte value) throws JMSException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setShortProperty(String name, short value) throws JMSException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setIntProperty(String name, int value) throws JMSException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setLongProperty(String name, long value) throws JMSException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setFloatProperty(String name, float value) throws JMSException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setDoubleProperty(String name, double value) throws JMSException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setStringProperty(String name, String value) throws JMSException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setObjectProperty(String name, Object value) throws JMSException {
        // TODO Auto-generated method stub

    }

    @Override
    public void acknowledge() throws JMSException {

    }

    @Override
    public void clearBody() throws JMSException {

    }

    @Override
    public boolean isBodyAssignableTo(@SuppressWarnings("rawtypes") Class target) throws JMSException {
        return true;
    }

    @Override
    public final <T> T getBody(Class<T> asType) throws JMSException {
        if (isBodyAssignableTo(asType)) {
            return doGetBody(asType);
        }

        throw new MessageFormatException("Message body cannot be read as type: " + asType);
    }

    public boolean isReadOnly() {
        return this.readOnly;
    }

    public void setReadOnly(boolean readOnly) {
        this.readOnly = readOnly;
    }

    public boolean isReadOnlyBody() {
        return this.readOnlyBody;
    }

    public void setReadOnlyBody(boolean readOnlyBody) {
        this.readOnlyBody = readOnlyBody;
    }

    public boolean isReadOnlyProperties() {
        return this.readOnlyProperties;
    }

    public void setReadOnlyProperties(boolean readOnlyProperties) {
        this.readOnlyProperties = readOnlyProperties;
    }

    protected <T> T doGetBody(Class<T> asType) throws JMSException {
        return null;
    }

    protected void checkReadOnly() throws MessageNotWriteableException {
        if (readOnly) {
            throw new MessageNotWriteableException("Message is currently read-only");
        }
    }

    protected void checkReadOnlyProperties() throws MessageNotWriteableException {
        if (readOnly || readOnlyProperties) {
            throw new MessageNotWriteableException("Message properties are read-only");
        }
    }

    protected void checkReadOnlyBody() throws MessageNotWriteableException {
        if (readOnly || readOnlyBody) {
            throw new MessageNotWriteableException("Message body is read-only");
        }
    }

    protected void checkWriteOnlyBody() throws MessageNotReadableException {
        if (!readOnlyBody) {
            throw new MessageNotReadableException("Message body is write-only");
        }
    }
}
