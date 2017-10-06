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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.jms.CompletionListener;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.IllegalStateException;
import javax.jms.IllegalStateRuntimeException;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.JMSProducer;
import javax.jms.JMSRuntimeException;
import javax.jms.Message;
import javax.jms.MessageFormatRuntimeException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.messaginghub.pooled.jms.JmsPoolJMSContext;
import org.messaginghub.pooled.jms.JmsPoolJMSProducer;
import org.messaginghub.pooled.jms.mock.MockJMSConnection;
import org.messaginghub.pooled.jms.mock.MockJMSConnectionListener;
import org.messaginghub.pooled.jms.mock.MockJMSMessageProducer;
import org.messaginghub.pooled.jms.mock.MockJMSSession;
import org.messaginghub.pooled.jms.mock.MockJMSTopic;

/**
 * Tests for the JMSProducer implementation provided by the JMS Pool
 */
public class JmsPoolJMSProducerTest extends JmsPoolTestSupport {

    private final String STRING_PROPERTY_NAME = "StringProperty";
    private final String STRING_PROPERTY_VALUE = UUID.randomUUID().toString();

    private final String BYTE_PROPERTY_NAME = "ByteProperty";
    private final byte BYTE_PROPERTY_VALUE = Byte.MAX_VALUE;

    private final String BOOLEAN_PROPERTY_NAME = "BooleanProperty";
    private final boolean BOOLEAN_PROPERTY_VALUE = Boolean.TRUE;

    private final String SHORT_PROPERTY_NAME = "ShortProperty";
    private final short SHORT_PROPERTY_VALUE = Short.MAX_VALUE;

    private final String INTEGER_PROPERTY_NAME = "IntegerProperty";
    private final int INTEGER_PROPERTY_VALUE = Integer.MAX_VALUE;

    private final String LONG_PROPERTY_NAME = "LongProperty";
    private final long LONG_PROPERTY_VALUE = Long.MAX_VALUE;

    private final String DOUBLE_PROPERTY_NAME = "DoubleProperty";
    private final double DOUBLE_PROPERTY_VALUE = Double.MAX_VALUE;

    private final String FLOAT_PROPERTY_NAME = "FloatProperty";
    private final float FLOAT_PROPERTY_VALUE = Float.MAX_VALUE;

    private final String BAD_PROPERTY_NAME = "%_BAD_PROPERTY_NAME";
    private final String GOOD_PROPERTY_NAME = "GOOD_PROPERTY_NAME";

    private final Destination JMS_DESTINATION = new MockJMSTopic("test.target.topic:001");
    private final Destination JMS_REPLY_TO = new MockJMSTopic("test.replyto.topic:001");
    private final String JMS_CORRELATION_ID = UUID.randomUUID().toString();
    private final String JMS_TYPE_STRING = "TestType";

    private JmsPoolJMSContext context;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        context = (JmsPoolJMSContext) cf.createContext();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        try {
            context.close();
        } finally {
            super.tearDown();
        }
    }

    //----- Test basic functionality -----------------------------------------//

    @Test(timeout = 30000)
    public void testCreateJMSProducer() throws JMSException {
        JmsPoolJMSProducer producer = (JmsPoolJMSProducer) context.createProducer();
        assertNotNull(producer);
        MockJMSMessageProducer mockProducer = (MockJMSMessageProducer) producer.getMessageProducer();
        assertNotNull(mockProducer);

        // JMSProducer instances are always anonymous producers.
        assertNull(mockProducer.getDestination());

        context.close();

        try {
            producer.getMessageProducer();
            fail("should throw on closed context.");
        } catch (JMSRuntimeException jmsre) {}
    }

    @Test(timeout = 30000)
    public void testToString() throws JMSException {
        JMSProducer producer = context.createProducer();
        assertNotNull(producer.toString());
    }

    //----- Test Property Handling methods -----------------------------------//

    @Test
    public void testGetPropertyNames() {
        JMSProducer producer = context.createProducer();

        producer.setProperty("Property_1", "1");
        producer.setProperty("Property_2", "2");
        producer.setProperty("Property_3", "3");

        assertEquals(3, producer.getPropertyNames().size());

        assertTrue(producer.getPropertyNames().contains("Property_1"));
        assertTrue(producer.getPropertyNames().contains("Property_2"));
        assertTrue(producer.getPropertyNames().contains("Property_3"));
    }

    @Test
    public void testClearProperties() {
        JMSProducer producer = context.createProducer();

        producer.setProperty("Property_1", "1");
        producer.setProperty("Property_2", "2");
        producer.setProperty("Property_3", "3");

        assertEquals(3, producer.getPropertyNames().size());

        producer.clearProperties();

        assertEquals(0, producer.getPropertyNames().size());
    }

    @Test
    public void testPropertyExists() {
        JMSProducer producer = context.createProducer();

        producer.setProperty("Property_1", "1");
        producer.setProperty("Property_2", "2");
        producer.setProperty("Property_3", "3");

        assertEquals(3, producer.getPropertyNames().size());

        assertTrue(producer.propertyExists("Property_1"));
        assertTrue(producer.propertyExists("Property_2"));
        assertTrue(producer.propertyExists("Property_3"));
        assertFalse(producer.propertyExists("Property_4"));
    }

    //----- Test for JMS Message Headers are stored --------------------------//

    @Test
    public void testJMSCorrelationID() {
        JMSProducer producer = context.createProducer();

        producer.setJMSCorrelationID(JMS_CORRELATION_ID);
        assertEquals(JMS_CORRELATION_ID, producer.getJMSCorrelationID());
    }

    @Test
    public void testJMSCorrelationIDBytes() {
        JMSProducer producer = context.createProducer();

        producer.setJMSCorrelationIDAsBytes(JMS_CORRELATION_ID.getBytes(StandardCharsets.UTF_8));
        assertEquals(JMS_CORRELATION_ID, new String(producer.getJMSCorrelationIDAsBytes(), StandardCharsets.UTF_8));
    }

    @Test
    public void testJMSReplyTo() {
        JMSContext context = cf.createContext();
        JMSProducer producer = context.createProducer();

        producer.setJMSReplyTo(JMS_REPLY_TO);
        assertTrue(JMS_REPLY_TO.equals(producer.getJMSReplyTo()));
    }

    @Test
    public void testJMSType() {
        JMSProducer producer = context.createProducer();

        producer.setJMSType(JMS_TYPE_STRING);
        assertEquals(JMS_TYPE_STRING, producer.getJMSType());
    }

    //----- Test for get property on matching types --------------------------//

    @Test
    public void testGetStringPropetry() {
        JMSProducer producer = context.createProducer();
        producer.setProperty(STRING_PROPERTY_NAME, STRING_PROPERTY_VALUE);
        assertEquals(STRING_PROPERTY_VALUE, producer.getStringProperty(STRING_PROPERTY_NAME));
    }

    @Test
    public void testGetBytePropetry() {
        JMSProducer producer = context.createProducer();
        producer.setProperty(BYTE_PROPERTY_NAME, BYTE_PROPERTY_VALUE);
        assertEquals(BYTE_PROPERTY_VALUE, producer.getByteProperty(BYTE_PROPERTY_NAME));
    }

    @Test
    public void testGetBooleanPropetry() {
        JMSProducer producer = context.createProducer();
        producer.setProperty(BOOLEAN_PROPERTY_NAME, BOOLEAN_PROPERTY_VALUE);
        assertEquals(BOOLEAN_PROPERTY_VALUE, producer.getBooleanProperty(BOOLEAN_PROPERTY_NAME));
    }

    @Test
    public void testGetShortPropetry() {
        JMSProducer producer = context.createProducer();
        producer.setProperty(SHORT_PROPERTY_NAME, SHORT_PROPERTY_VALUE);
        assertEquals(SHORT_PROPERTY_VALUE, producer.getShortProperty(SHORT_PROPERTY_NAME));
    }

    @Test
    public void testGetIntegerPropetry() {
        JMSProducer producer = context.createProducer();
        producer.setProperty(INTEGER_PROPERTY_NAME, INTEGER_PROPERTY_VALUE);
        assertEquals(INTEGER_PROPERTY_VALUE, producer.getIntProperty(INTEGER_PROPERTY_NAME));
    }

    @Test
    public void testGetLongPropetry() {
        JMSProducer producer = context.createProducer();
        producer.setProperty(LONG_PROPERTY_NAME, LONG_PROPERTY_VALUE);
        assertEquals(LONG_PROPERTY_VALUE, producer.getLongProperty(LONG_PROPERTY_NAME));
    }

    @Test
    public void testGetDoublePropetry() {
        JMSProducer producer = context.createProducer();
        producer.setProperty(DOUBLE_PROPERTY_NAME, DOUBLE_PROPERTY_VALUE);
        assertEquals(DOUBLE_PROPERTY_VALUE, producer.getDoubleProperty(DOUBLE_PROPERTY_NAME), 0.0);
    }

    @Test
    public void testGetFloatPropetry() {
        JMSProducer producer = context.createProducer();
        producer.setProperty(FLOAT_PROPERTY_NAME, FLOAT_PROPERTY_VALUE);
        assertEquals(FLOAT_PROPERTY_VALUE, producer.getFloatProperty(FLOAT_PROPERTY_NAME), 0.0f);
    }

    //----- Test for set property handling -----------------------------------//

    @Test
    public void testSetPropertyConversions() {
        JMSProducer producer = context.createProducer();

        producer.setProperty(STRING_PROPERTY_NAME, STRING_PROPERTY_VALUE);
        producer.setProperty(BYTE_PROPERTY_NAME, Byte.valueOf(BYTE_PROPERTY_VALUE));
        producer.setProperty(BOOLEAN_PROPERTY_NAME, Boolean.valueOf(BOOLEAN_PROPERTY_VALUE));
        producer.setProperty(SHORT_PROPERTY_NAME, Short.valueOf(SHORT_PROPERTY_VALUE));
        producer.setProperty(INTEGER_PROPERTY_NAME, Integer.valueOf(INTEGER_PROPERTY_VALUE));
        producer.setProperty(LONG_PROPERTY_NAME, Long.valueOf(LONG_PROPERTY_VALUE));
        producer.setProperty(FLOAT_PROPERTY_NAME, Float.valueOf(FLOAT_PROPERTY_VALUE));
        producer.setProperty(DOUBLE_PROPERTY_NAME, Double.valueOf(DOUBLE_PROPERTY_VALUE));

        try {
            producer.setProperty(STRING_PROPERTY_NAME, UUID.randomUUID());
            fail("Should not be able to set non-primitive type");
        } catch (MessageFormatRuntimeException mfe) {
        }

        assertNull(producer.getObjectProperty("Unknown"));

        assertEquals(STRING_PROPERTY_VALUE, producer.getStringProperty(STRING_PROPERTY_NAME));
        assertEquals(BYTE_PROPERTY_VALUE, producer.getByteProperty(BYTE_PROPERTY_NAME));
        assertEquals(BOOLEAN_PROPERTY_VALUE, producer.getBooleanProperty(BOOLEAN_PROPERTY_NAME));
        assertEquals(SHORT_PROPERTY_VALUE, producer.getShortProperty(SHORT_PROPERTY_NAME));
        assertEquals(INTEGER_PROPERTY_VALUE, producer.getIntProperty(INTEGER_PROPERTY_NAME));
        assertEquals(LONG_PROPERTY_VALUE, producer.getLongProperty(LONG_PROPERTY_NAME));
        assertEquals(FLOAT_PROPERTY_VALUE, producer.getFloatProperty(FLOAT_PROPERTY_NAME), 0.0);
        assertEquals(DOUBLE_PROPERTY_VALUE, producer.getDoubleProperty(DOUBLE_PROPERTY_NAME), 0.0);

        assertEquals(STRING_PROPERTY_VALUE, producer.getObjectProperty(STRING_PROPERTY_NAME));
        assertEquals(BYTE_PROPERTY_VALUE, producer.getObjectProperty(BYTE_PROPERTY_NAME));
        assertEquals(BOOLEAN_PROPERTY_VALUE, producer.getObjectProperty(BOOLEAN_PROPERTY_NAME));
        assertEquals(SHORT_PROPERTY_VALUE, producer.getObjectProperty(SHORT_PROPERTY_NAME));
        assertEquals(INTEGER_PROPERTY_VALUE, producer.getObjectProperty(INTEGER_PROPERTY_NAME));
        assertEquals(LONG_PROPERTY_VALUE, producer.getObjectProperty(LONG_PROPERTY_NAME));
        assertEquals(FLOAT_PROPERTY_VALUE, producer.getObjectProperty(FLOAT_PROPERTY_NAME));
        assertEquals(DOUBLE_PROPERTY_VALUE, producer.getObjectProperty(DOUBLE_PROPERTY_NAME));
    }

    //----- Test for get property conversions --------------------------------//

    @Test
    public void testStringPropertyConversions() {
        JMSProducer producer = context.createProducer();
        producer.setProperty(STRING_PROPERTY_NAME, STRING_PROPERTY_VALUE);
        producer.setProperty(BYTE_PROPERTY_NAME, BYTE_PROPERTY_VALUE);
        producer.setProperty(BOOLEAN_PROPERTY_NAME, BOOLEAN_PROPERTY_VALUE);
        producer.setProperty(SHORT_PROPERTY_NAME, SHORT_PROPERTY_VALUE);
        producer.setProperty(INTEGER_PROPERTY_NAME, INTEGER_PROPERTY_VALUE);
        producer.setProperty(LONG_PROPERTY_NAME, LONG_PROPERTY_VALUE);
        producer.setProperty(FLOAT_PROPERTY_NAME, FLOAT_PROPERTY_VALUE);
        producer.setProperty(DOUBLE_PROPERTY_NAME, DOUBLE_PROPERTY_VALUE);

        assertEquals(String.valueOf(STRING_PROPERTY_VALUE), producer.getStringProperty(STRING_PROPERTY_NAME));
        assertEquals(String.valueOf(BYTE_PROPERTY_VALUE), producer.getStringProperty(BYTE_PROPERTY_NAME));
        assertEquals(String.valueOf(BOOLEAN_PROPERTY_VALUE), producer.getStringProperty(BOOLEAN_PROPERTY_NAME));
        assertEquals(String.valueOf(SHORT_PROPERTY_VALUE), producer.getStringProperty(SHORT_PROPERTY_NAME));
        assertEquals(String.valueOf(INTEGER_PROPERTY_VALUE), producer.getStringProperty(INTEGER_PROPERTY_NAME));
        assertEquals(String.valueOf(LONG_PROPERTY_VALUE), producer.getStringProperty(LONG_PROPERTY_NAME));
        assertEquals(String.valueOf(FLOAT_PROPERTY_VALUE), producer.getStringProperty(FLOAT_PROPERTY_NAME));
        assertEquals(String.valueOf(DOUBLE_PROPERTY_VALUE), producer.getStringProperty(DOUBLE_PROPERTY_NAME));
    }

    @Test
    public void testBytePropertyConversions() {
        JMSProducer producer = context.createProducer();
        producer.setProperty(STRING_PROPERTY_NAME, STRING_PROPERTY_VALUE);
        producer.setProperty(BYTE_PROPERTY_NAME, BYTE_PROPERTY_VALUE);
        producer.setProperty(BOOLEAN_PROPERTY_NAME, BOOLEAN_PROPERTY_VALUE);
        producer.setProperty(SHORT_PROPERTY_NAME, SHORT_PROPERTY_VALUE);
        producer.setProperty(INTEGER_PROPERTY_NAME, INTEGER_PROPERTY_VALUE);
        producer.setProperty(LONG_PROPERTY_NAME, LONG_PROPERTY_VALUE);
        producer.setProperty(FLOAT_PROPERTY_NAME, FLOAT_PROPERTY_VALUE);
        producer.setProperty(DOUBLE_PROPERTY_NAME, DOUBLE_PROPERTY_VALUE);

        assertEquals(BYTE_PROPERTY_VALUE, producer.getByteProperty(BYTE_PROPERTY_NAME));

        try {
            producer.getByteProperty(STRING_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (NumberFormatException nfe) {
        }
        try {
            producer.getByteProperty(BOOLEAN_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getByteProperty(SHORT_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getByteProperty(INTEGER_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getByteProperty(LONG_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getByteProperty(FLOAT_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getByteProperty(DOUBLE_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
    }

    @Test
    public void testBooleanPropertyConversions() {
        JMSProducer producer = context.createProducer();
        producer.setProperty(STRING_PROPERTY_NAME, STRING_PROPERTY_VALUE);
        producer.setProperty(BYTE_PROPERTY_NAME, BYTE_PROPERTY_VALUE);
        producer.setProperty(BOOLEAN_PROPERTY_NAME, BOOLEAN_PROPERTY_VALUE);
        producer.setProperty(SHORT_PROPERTY_NAME, SHORT_PROPERTY_VALUE);
        producer.setProperty(INTEGER_PROPERTY_NAME, INTEGER_PROPERTY_VALUE);
        producer.setProperty(LONG_PROPERTY_NAME, LONG_PROPERTY_VALUE);
        producer.setProperty(FLOAT_PROPERTY_NAME, FLOAT_PROPERTY_VALUE);
        producer.setProperty(DOUBLE_PROPERTY_NAME, DOUBLE_PROPERTY_VALUE);

        assertEquals(BOOLEAN_PROPERTY_VALUE, producer.getBooleanProperty(BOOLEAN_PROPERTY_NAME));
        assertEquals(Boolean.FALSE, producer.getBooleanProperty(STRING_PROPERTY_NAME));

        try {
            producer.getBooleanProperty(BYTE_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getBooleanProperty(SHORT_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getBooleanProperty(INTEGER_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getBooleanProperty(LONG_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getBooleanProperty(FLOAT_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getBooleanProperty(DOUBLE_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
    }

    @Test
    public void testShortPropertyConversions() {
        JMSProducer producer = context.createProducer();
        producer.setProperty(STRING_PROPERTY_NAME, STRING_PROPERTY_VALUE);
        producer.setProperty(BYTE_PROPERTY_NAME, BYTE_PROPERTY_VALUE);
        producer.setProperty(BOOLEAN_PROPERTY_NAME, BOOLEAN_PROPERTY_VALUE);
        producer.setProperty(SHORT_PROPERTY_NAME, SHORT_PROPERTY_VALUE);
        producer.setProperty(INTEGER_PROPERTY_NAME, INTEGER_PROPERTY_VALUE);
        producer.setProperty(LONG_PROPERTY_NAME, LONG_PROPERTY_VALUE);
        producer.setProperty(FLOAT_PROPERTY_NAME, FLOAT_PROPERTY_VALUE);
        producer.setProperty(DOUBLE_PROPERTY_NAME, DOUBLE_PROPERTY_VALUE);

        assertEquals(BYTE_PROPERTY_VALUE, producer.getShortProperty(BYTE_PROPERTY_NAME));
        assertEquals(SHORT_PROPERTY_VALUE, producer.getShortProperty(SHORT_PROPERTY_NAME));

        try {
            producer.getShortProperty(STRING_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (NumberFormatException nfe) {
        }
        try {
            producer.getShortProperty(BOOLEAN_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getShortProperty(INTEGER_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getShortProperty(LONG_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getShortProperty(FLOAT_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getShortProperty(DOUBLE_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
    }

    @Test
    public void testIntPropertyConversions() {
        JMSProducer producer = context.createProducer();
        producer.setProperty(STRING_PROPERTY_NAME, STRING_PROPERTY_VALUE);
        producer.setProperty(BYTE_PROPERTY_NAME, BYTE_PROPERTY_VALUE);
        producer.setProperty(BOOLEAN_PROPERTY_NAME, BOOLEAN_PROPERTY_VALUE);
        producer.setProperty(SHORT_PROPERTY_NAME, SHORT_PROPERTY_VALUE);
        producer.setProperty(INTEGER_PROPERTY_NAME, INTEGER_PROPERTY_VALUE);
        producer.setProperty(LONG_PROPERTY_NAME, LONG_PROPERTY_VALUE);
        producer.setProperty(FLOAT_PROPERTY_NAME, FLOAT_PROPERTY_VALUE);
        producer.setProperty(DOUBLE_PROPERTY_NAME, DOUBLE_PROPERTY_VALUE);

        assertEquals(BYTE_PROPERTY_VALUE, producer.getIntProperty(BYTE_PROPERTY_NAME));
        assertEquals(SHORT_PROPERTY_VALUE, producer.getIntProperty(SHORT_PROPERTY_NAME));
        assertEquals(INTEGER_PROPERTY_VALUE, producer.getIntProperty(INTEGER_PROPERTY_NAME));

        try {
            producer.getIntProperty(STRING_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (NumberFormatException nfe) {
        }
        try {
            producer.getIntProperty(BOOLEAN_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getIntProperty(LONG_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getIntProperty(FLOAT_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getIntProperty(DOUBLE_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
    }

    @Test
    public void testLongPropertyConversions() {
        JMSProducer producer = context.createProducer();
        producer.setProperty(STRING_PROPERTY_NAME, STRING_PROPERTY_VALUE);
        producer.setProperty(BYTE_PROPERTY_NAME, BYTE_PROPERTY_VALUE);
        producer.setProperty(BOOLEAN_PROPERTY_NAME, BOOLEAN_PROPERTY_VALUE);
        producer.setProperty(SHORT_PROPERTY_NAME, SHORT_PROPERTY_VALUE);
        producer.setProperty(INTEGER_PROPERTY_NAME, INTEGER_PROPERTY_VALUE);
        producer.setProperty(LONG_PROPERTY_NAME, LONG_PROPERTY_VALUE);
        producer.setProperty(FLOAT_PROPERTY_NAME, FLOAT_PROPERTY_VALUE);
        producer.setProperty(DOUBLE_PROPERTY_NAME, DOUBLE_PROPERTY_VALUE);

        assertEquals(BYTE_PROPERTY_VALUE, producer.getLongProperty(BYTE_PROPERTY_NAME));
        assertEquals(SHORT_PROPERTY_VALUE, producer.getLongProperty(SHORT_PROPERTY_NAME));
        assertEquals(INTEGER_PROPERTY_VALUE, producer.getLongProperty(INTEGER_PROPERTY_NAME));
        assertEquals(LONG_PROPERTY_VALUE, producer.getLongProperty(LONG_PROPERTY_NAME));

        try {
            producer.getLongProperty(STRING_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (NumberFormatException nfe) {
        }
        try {
            producer.getLongProperty(BOOLEAN_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getLongProperty(FLOAT_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getLongProperty(DOUBLE_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
    }

    @Test
    public void testFloatPropertyConversions() {
        JMSProducer producer = context.createProducer();
        producer.setProperty(STRING_PROPERTY_NAME, STRING_PROPERTY_VALUE);
        producer.setProperty(BYTE_PROPERTY_NAME, BYTE_PROPERTY_VALUE);
        producer.setProperty(BOOLEAN_PROPERTY_NAME, BOOLEAN_PROPERTY_VALUE);
        producer.setProperty(SHORT_PROPERTY_NAME, SHORT_PROPERTY_VALUE);
        producer.setProperty(INTEGER_PROPERTY_NAME, INTEGER_PROPERTY_VALUE);
        producer.setProperty(LONG_PROPERTY_NAME, LONG_PROPERTY_VALUE);
        producer.setProperty(FLOAT_PROPERTY_NAME, FLOAT_PROPERTY_VALUE);
        producer.setProperty(DOUBLE_PROPERTY_NAME, DOUBLE_PROPERTY_VALUE);

        assertEquals(FLOAT_PROPERTY_VALUE, producer.getFloatProperty(FLOAT_PROPERTY_NAME), 0.0f);

        try {
            producer.getFloatProperty(STRING_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (NumberFormatException nfe) {
        }
        try {
            producer.getFloatProperty(BOOLEAN_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getFloatProperty(SHORT_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getFloatProperty(INTEGER_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getFloatProperty(LONG_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getFloatProperty(BYTE_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getFloatProperty(DOUBLE_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
    }

    @Test
    public void testDoublePropertyConversions() {
        JMSProducer producer = context.createProducer();
        producer.setProperty(STRING_PROPERTY_NAME, STRING_PROPERTY_VALUE);
        producer.setProperty(BYTE_PROPERTY_NAME, BYTE_PROPERTY_VALUE);
        producer.setProperty(BOOLEAN_PROPERTY_NAME, BOOLEAN_PROPERTY_VALUE);
        producer.setProperty(SHORT_PROPERTY_NAME, SHORT_PROPERTY_VALUE);
        producer.setProperty(INTEGER_PROPERTY_NAME, INTEGER_PROPERTY_VALUE);
        producer.setProperty(LONG_PROPERTY_NAME, LONG_PROPERTY_VALUE);
        producer.setProperty(FLOAT_PROPERTY_NAME, FLOAT_PROPERTY_VALUE);
        producer.setProperty(DOUBLE_PROPERTY_NAME, DOUBLE_PROPERTY_VALUE);

        assertEquals(DOUBLE_PROPERTY_VALUE, producer.getDoubleProperty(DOUBLE_PROPERTY_NAME), 0.0);
        assertEquals(FLOAT_PROPERTY_VALUE, producer.getDoubleProperty(FLOAT_PROPERTY_NAME), 0.0);

        try {
            producer.getDoubleProperty(STRING_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (NumberFormatException nfe) {
        }
        try {
            producer.getDoubleProperty(BOOLEAN_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getDoubleProperty(SHORT_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getDoubleProperty(INTEGER_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getDoubleProperty(LONG_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getDoubleProperty(BYTE_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
    }

    //----- Test for error when set called with invalid name -----------------//

    @Test
    public void testSetStringPropetryWithBadPropetyName() {
        JMSProducer producer = context.createProducer();

        try {
            producer.setProperty(BAD_PROPERTY_NAME, "X");
            fail("Should not accept invalid property name");
        } catch (IllegalArgumentException iae) {}
    }

    @Test
    public void testSetBytePropetryWithBadPropetyName() {
        JMSProducer producer = context.createProducer();

        try {
            producer.setProperty(BAD_PROPERTY_NAME, (byte) 1);
            fail("Should not accept invalid property name");
        } catch (IllegalArgumentException iae) {}
    }

    @Test
    public void testSetBooleanPropetryWithBadPropetyName() {
        JMSProducer producer = context.createProducer();

        try {
            producer.setProperty(BAD_PROPERTY_NAME, true);
            fail("Should not accept invalid property name");
        } catch (IllegalArgumentException iae) {}
    }

    @Test
    public void testSetDoublePropetryWithBadPropetyName() {
        JMSProducer producer = context.createProducer();

        try {
            producer.setProperty(BAD_PROPERTY_NAME, 100.0);
            fail("Should not accept invalid property name");
        } catch (IllegalArgumentException iae) {}
    }

    @Test
    public void testSetFloatPropetryWithBadPropetyName() {
        JMSProducer producer = context.createProducer();

        try {
            producer.setProperty(BAD_PROPERTY_NAME, 100.0f);
            fail("Should not accept invalid property name");
        } catch (IllegalArgumentException iae) {}
    }

    @Test
    public void testSetShortPropetryWithBadPropetyName() {
        JMSProducer producer = context.createProducer();

        try {
            producer.setProperty(BAD_PROPERTY_NAME, (short) 100);
            fail("Should not accept invalid property name");
        } catch (IllegalArgumentException iae) {}
    }

    @Test
    public void testSetIntPropetryWithBadPropetyName() {
        JMSProducer producer = context.createProducer();

        try {
            producer.setProperty(BAD_PROPERTY_NAME, 100);
            fail("Should not accept invalid property name");
        } catch (IllegalArgumentException iae) {}
    }

    @Test
    public void testSetLongPropetryWithBadPropetyName() {
        JMSProducer producer = context.createProducer();

        try {
            producer.setProperty(BAD_PROPERTY_NAME, 100l);
            fail("Should not accept invalid property name");
        } catch (IllegalArgumentException iae) {}
    }

    @Test
    public void testSetObjectPropetryWithBadPropetyName() {
        JMSProducer producer = context.createProducer();

        try {
            producer.setProperty(BAD_PROPERTY_NAME, UUID.randomUUID());
            fail("Should not accept invalid property name");
        } catch (IllegalArgumentException iae) {}
    }

    @Test
    public void testSetObjectPropetryWithInvalidObject() {
        JMSProducer producer = context.createProducer();

        try {
            producer.setProperty(GOOD_PROPERTY_NAME, UUID.randomUUID());
            fail("Should not accept invalid property name");
        } catch (MessageFormatRuntimeException mfre) {}
    }

    //----- Tests for producer send configuration methods --------------------//

    @Test
    public void testAsync() {
        JMSProducer producer = context.createProducer();
        TestJmsCompletionListener listener = new TestJmsCompletionListener();

        producer.setAsync(listener);
        assertEquals(listener, producer.getAsync());
    }

    @Test
    public void testDeliveryMode() {
        JMSProducer producer = context.createProducer();

        producer.setDeliveryMode(DeliveryMode.PERSISTENT);
        assertEquals(DeliveryMode.PERSISTENT, producer.getDeliveryMode());
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        assertEquals(DeliveryMode.NON_PERSISTENT, producer.getDeliveryMode());
    }

    @Test(timeout = 10000)
    public void testDeliveryModeConfigurationWithInvalidMode() throws Exception {
        JMSProducer producer = context.createProducer();

        assertEquals(Message.DEFAULT_DELIVERY_MODE, producer.getDeliveryMode());

        try {
            producer.setDeliveryMode(-1);
            fail("Should have thrown an exception");
        } catch (JMSRuntimeException ex) {
            // Expected
        }

        try {
            producer.setDeliveryMode(5);
            fail("Should have thrown an exception");
        } catch (JMSRuntimeException ex) {
            // Expected
        }

        assertEquals(Message.DEFAULT_DELIVERY_MODE, producer.getDeliveryMode());
    }

    @Test
    public void testDeliveryDelay() {
        JMSProducer producer = context.createProducer();

        assertEquals(0, producer.getDeliveryDelay());
        try {
            producer.setDeliveryDelay(2000);
            fail("Pool JMSProducer can't modify shared session delay mode.");
        } catch (JMSRuntimeException jmsre) {
        }
    }

    @Test
    public void testDisableMessageID() {
        JMSProducer producer = context.createProducer();

        assertEquals(false, producer.getDisableMessageID());
        try {
            producer.setDisableMessageID(true);
            fail("Pool JMSProducer can't modify shared session disable mode.");
        } catch (JMSRuntimeException jmsre) {
        }
        try {
            producer.setDisableMessageID(false);
            fail("Pool JMSProducer can't modify shared session disable mode.");
        } catch (JMSRuntimeException jmsre) {
        }
    }

    @Test
    public void testMessageDisableTimestamp() {
        JMSProducer producer = context.createProducer();

        assertEquals(false, producer.getDisableMessageTimestamp());
        try {
            producer.setDisableMessageTimestamp(true);
            fail("Pool JMSProducer can't modify shared session disable mode.");
        } catch (JMSRuntimeException jmsre) {
        }
        try {
            producer.setDisableMessageTimestamp(false);
            fail("Pool JMSProducer can't modify shared session disable mode.");
        } catch (JMSRuntimeException jmsre) {
        }
    }

    @Test
    public void testPriority() {
        JMSProducer producer = context.createProducer();

        producer.setPriority(1);
        assertEquals(1, producer.getPriority());
        producer.setPriority(4);
        assertEquals(4, producer.getPriority());
    }

    @Test(timeout = 10000)
    public void testPriorityConfigurationWithInvalidPriorityValues() throws Exception {
        JMSProducer producer = context.createProducer();

        assertEquals(Message.DEFAULT_PRIORITY, producer.getPriority());

        try {
            producer.setPriority(-1);
            fail("Should have thrown an exception");
        } catch (JMSRuntimeException ex) {
            // Expected
        }

        try {
            producer.setPriority(10);
            fail("Should have thrown an exception");
        } catch (JMSRuntimeException ex) {
            // Expected
        }

        assertEquals(Message.DEFAULT_PRIORITY, producer.getPriority());
    }

    @Test
    public void testTimeToLive() {
        JMSProducer producer = context.createProducer();

        producer.setTimeToLive(2000);
        assertEquals(2000, producer.getTimeToLive());
    }

    //----- Test Send Methods -----------------------------------------------//

    @Test
    public void testSendNullMessageThrowsMFRE() throws JMSException {
        JMSProducer producer = context.createProducer();

        try {
            producer.send(JMS_DESTINATION, (Message) null);
            fail("Should throw a MessageFormatRuntimeException");
        } catch (MessageFormatRuntimeException mfre) {
        } catch (Exception e) {
            fail("Should throw a MessageFormatRuntimeException");
        }
    }

    @Test
    public void testSendJMSMessage() throws JMSException {
        JMSProducer producer = context.createProducer();
        Message message = context.createMessage();

        final AtomicBoolean messageSent = new AtomicBoolean();

        MockJMSConnection connection = (MockJMSConnection) context.getConnection();
        connection.addConnectionListener(new MockJMSConnectionListener() {

            @Override
            public void onMessageSend(MockJMSSession session, Message message) throws JMSException {
                assertEquals(JMS_CORRELATION_ID, message.getJMSCorrelationID());
                assertTrue(Arrays.equals(JMS_CORRELATION_ID.getBytes(), message.getJMSCorrelationIDAsBytes()));
                assertEquals(JMS_REPLY_TO, message.getJMSReplyTo());
                assertEquals(JMS_TYPE_STRING, message.getJMSType());

                messageSent.set(true);
            }
        });

        producer.setJMSCorrelationID(JMS_CORRELATION_ID);
        producer.setJMSCorrelationIDAsBytes(JMS_CORRELATION_ID.getBytes());
        producer.setJMSReplyTo(JMS_REPLY_TO);
        producer.setJMSType(JMS_TYPE_STRING);

        producer.send(JMS_DESTINATION, message);

        assertTrue(messageSent.get());
    }

    @Test
    public void testSendAppliesDeliveryModeMessageBody() throws JMSException {
        doTestSendAppliesDeliveryModeWithMessageBody(Message.class);
    }

    @Test
    public void testSendAppliesDeliveryModeStringMessageBody() throws JMSException {
        doTestSendAppliesDeliveryModeWithMessageBody(String.class);
    }

    @Test
    public void testSendAppliesDeliveryModeMapMessageBody() throws JMSException {
        doTestSendAppliesDeliveryModeWithMessageBody(Map.class);
    }

    @Test
    public void testSendAppliesDeliveryModeBytesMessageBody() throws JMSException {
        doTestSendAppliesDeliveryModeWithMessageBody(byte[].class);
    }

    @Test
    public void testSendAppliesDeliveryModeSerializableMessageBody() throws JMSException {
        doTestSendAppliesDeliveryModeWithMessageBody(UUID.class);
    }

    public void doTestSendAppliesDeliveryModeWithMessageBody(Class<?> bodyType) throws JMSException {
        JMSProducer producer = context.createProducer();

        final AtomicBoolean nonPersistentMessage = new AtomicBoolean();
        final AtomicBoolean persistentMessage = new AtomicBoolean();

        MockJMSConnection connection = (MockJMSConnection) context.getConnection();
        connection.addConnectionListener(new MockJMSConnectionListener() {

            @Override
            public void onMessageSend(MockJMSSession session, Message message) throws JMSException {
                if (!persistentMessage.get()) {
                    assertEquals(DeliveryMode.PERSISTENT, message.getJMSDeliveryMode());
                    persistentMessage.set(true);
                } else {
                    assertEquals(DeliveryMode.NON_PERSISTENT, message.getJMSDeliveryMode());
                    nonPersistentMessage.set(true);
                }
            }
        });

        producer.setDeliveryMode(DeliveryMode.PERSISTENT);
        producer.send(JMS_DESTINATION, "text");

        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        producer.send(JMS_DESTINATION, "text");

        assertTrue(persistentMessage.get());
        assertTrue(nonPersistentMessage.get());
    }

    @Test
    public void testSendAppliesPrioirtyMessageBody() throws JMSException {
        doTestSendAppliesPriorityWithMessageBody(Message.class);
    }

    @Test
    public void testSendAppliesPriorityStringMessageBody() throws JMSException {
        doTestSendAppliesPriorityWithMessageBody(String.class);
    }

    @Test
    public void testSendAppliesPriorityMapMessageBody() throws JMSException {
        doTestSendAppliesPriorityWithMessageBody(Map.class);
    }

    @Test
    public void testSendAppliesPriorityBytesMessageBody() throws JMSException {
        doTestSendAppliesPriorityWithMessageBody(byte[].class);
    }

    @Test
    public void testSendAppliesPrioritySerializableMessageBody() throws JMSException {
        doTestSendAppliesPriorityWithMessageBody(UUID.class);
    }

    private void doTestSendAppliesPriorityWithMessageBody(Class<?> bodyType) throws JMSException {
        JMSProducer producer = context.createProducer();

        final AtomicBoolean lowPriority = new AtomicBoolean();
        final AtomicBoolean highPriority = new AtomicBoolean();

        MockJMSConnection connection = (MockJMSConnection) context.getConnection();
        connection.addConnectionListener(new MockJMSConnectionListener() {

            @Override
            public void onMessageSend(MockJMSSession session, Message message) throws JMSException {
                if (!lowPriority.get()) {
                    assertEquals(0, message.getJMSPriority());
                    lowPriority.set(true);
                } else {
                    assertEquals(7, message.getJMSPriority());
                    highPriority.set(true);
                }
            }
        });

        producer.setPriority(0);
        producer.send(JMS_DESTINATION, "text");

        producer.setPriority(7);
        producer.send(JMS_DESTINATION, "text");

        assertTrue(lowPriority.get());
        assertTrue(highPriority.get());
    }

    @Test
    public void testSendAppliesTimeToLiveMessageBody() throws JMSException {
        doTestSendAppliesTimeToLiveWithMessageBody(Message.class);
    }

    @Test
    public void testSendAppliesTimeToLiveStringMessageBody() throws JMSException {
        doTestSendAppliesTimeToLiveWithMessageBody(String.class);
    }

    @Test
    public void testSendAppliesTimeToLiveMapMessageBody() throws JMSException {
        doTestSendAppliesTimeToLiveWithMessageBody(Map.class);
    }

    @Test
    public void testSendAppliesTimeToLiveBytesMessageBody() throws JMSException {
        doTestSendAppliesTimeToLiveWithMessageBody(byte[].class);
    }

    @Test
    public void testSendAppliesTimeToLiveSerializableMessageBody() throws JMSException {
        doTestSendAppliesTimeToLiveWithMessageBody(UUID.class);
    }

    private void doTestSendAppliesTimeToLiveWithMessageBody(Class<?> bodyType) throws JMSException {
        JMSProducer producer = context.createProducer();

        final AtomicBoolean nonDefaultTTL = new AtomicBoolean();
        final AtomicBoolean defaultTTL = new AtomicBoolean();

        MockJMSConnection connection = (MockJMSConnection) context.getConnection();
        connection.addConnectionListener(new MockJMSConnectionListener() {

            @Override
            public void onMessageSend(MockJMSSession session, Message message) throws JMSException {
                if (!nonDefaultTTL.get()) {
                    assertTrue(message.getJMSExpiration() > 0);
                    nonDefaultTTL.set(true);
                } else {
                    assertTrue(message.getJMSExpiration() == 0);
                    defaultTTL.set(true);
                }
            }
        });

        producer.setTimeToLive(2000);
        producer.send(JMS_DESTINATION, "text");

        producer.setTimeToLive(Message.DEFAULT_TIME_TO_LIVE);
        producer.send(JMS_DESTINATION, "text");

        assertTrue(nonDefaultTTL.get());
        assertTrue(defaultTTL.get());
    }

    @Test
    public void testStringBodyIsApplied() throws JMSException {
        JMSProducer producer = context.createProducer();

        final String bodyValue = "String-Value";
        final AtomicBoolean bodyValidated = new AtomicBoolean();

        MockJMSConnection connection = (MockJMSConnection) context.getConnection();
        connection.addConnectionListener(new MockJMSConnectionListener() {

            @Override
            public void onMessageSend(MockJMSSession session, Message message) throws JMSException {
                assertEquals(bodyValue, message.getBody(String.class));
                bodyValidated.set(true);
            }
        });

        producer.send(JMS_DESTINATION, bodyValue);
        assertTrue(bodyValidated.get());
    }

    @Test
    public void testMapBodyIsApplied() throws JMSException {
        JMSProducer producer = context.createProducer();

        final Map<String, Object> bodyValue = new HashMap<String, Object>();

        bodyValue.put("Value-1", "First");
        bodyValue.put("Value-2", "Second");

        final AtomicBoolean bodyValidated = new AtomicBoolean();

        MockJMSConnection connection = (MockJMSConnection) context.getConnection();
        connection.addConnectionListener(new MockJMSConnectionListener() {

            @Override
            public void onMessageSend(MockJMSSession session, Message message) throws JMSException {
                assertEquals(bodyValue, message.getBody(Map.class));
                bodyValidated.set(true);
            }
        });

        producer.send(JMS_DESTINATION, bodyValue);
        assertTrue(bodyValidated.get());
    }

    @Test
    public void testBytesBodyIsApplied() throws JMSException {
        JMSProducer producer = context.createProducer();

        final byte[] bodyValue = new byte[] { 0, 1, 2, 3, 4 };
        final AtomicBoolean bodyValidated = new AtomicBoolean();

        MockJMSConnection connection = (MockJMSConnection) context.getConnection();
        connection.addConnectionListener(new MockJMSConnectionListener() {

            @Override
            public void onMessageSend(MockJMSSession session, Message message) throws JMSException {
                byte[] payload = message.getBody(byte[].class);
                assertNotNull(payload);
                assertEquals(bodyValue.length, payload.length);

                for (int i = 0; i < payload.length; ++i) {
                    assertEquals(bodyValue[i], payload[i]);
                }

                bodyValidated.set(true);
            }
        });

        producer.send(JMS_DESTINATION, bodyValue);
        assertTrue(bodyValidated.get());
    }

    @Test
    public void testSerializableBodyIsApplied() throws JMSException {
        JMSProducer producer = context.createProducer();

        final UUID bodyValue = UUID.randomUUID();
        final AtomicBoolean bodyValidated = new AtomicBoolean();

        MockJMSConnection connection = (MockJMSConnection) context.getConnection();
        connection.addConnectionListener(new MockJMSConnectionListener() {

            @Override
            public void onMessageSend(MockJMSSession session, Message message) throws JMSException {
                assertEquals(bodyValue, message.getBody(UUID.class));
                bodyValidated.set(true);
            }
        });

        producer.send(JMS_DESTINATION, bodyValue);
        assertTrue(bodyValidated.get());
    }

    //----- Test for conversions to JMSRuntimeException ----------------------//

    @Test
    public void testRuntimeExceptionFromSendMessage() throws JMSException {
        JMSProducer producer = context.createProducer();

        MockJMSConnection connection = (MockJMSConnection) context.getConnection();
        connection.addConnectionListener(new MockJMSConnectionListener() {

            @Override
            public void onMessageSend(MockJMSSession session, Message message) throws JMSException {
                throw new IllegalStateException("Send Failed");
            }
        });

        try {
            producer.send(context.createTemporaryQueue(), context.createMessage());
            fail("Should have thrown an exception");
        } catch (IllegalStateRuntimeException isre) {}
    }

    @Test
    public void testRuntimeExceptionFromSendByteBody() throws JMSException {
        JMSProducer producer = context.createProducer();

        MockJMSConnection connection = (MockJMSConnection) context.getConnection();
        connection.addConnectionListener(new MockJMSConnectionListener() {

            @Override
            public void onMessageSend(MockJMSSession session, Message message) throws JMSException {
                throw new IllegalStateException("Send Failed");
            }
        });

        try {
            producer.send(context.createTemporaryQueue(), new byte[0]);
            fail("Should have thrown an exception");
        } catch (IllegalStateRuntimeException isre) {}
    }

    @Test
    public void testRuntimeExceptionFromSendMapBody() throws JMSException {
        JMSProducer producer = context.createProducer();

        MockJMSConnection connection = (MockJMSConnection) context.getConnection();
        connection.addConnectionListener(new MockJMSConnectionListener() {

            @Override
            public void onMessageSend(MockJMSSession session, Message message) throws JMSException {
                throw new IllegalStateException("Send Failed");
            }
        });

        try {
            producer.send(context.createTemporaryQueue(), Collections.<String, Object>emptyMap());
            fail("Should have thrown an exception");
        } catch (IllegalStateRuntimeException isre) {}
    }

    @Test
    public void testRuntimeExceptionFromSendSerializableBody() throws JMSException {
        JMSProducer producer = context.createProducer();

        MockJMSConnection connection = (MockJMSConnection) context.getConnection();
        connection.addConnectionListener(new MockJMSConnectionListener() {

            @Override
            public void onMessageSend(MockJMSSession session, Message message) throws JMSException {
                throw new IllegalStateException("Send Failed");
            }
        });

        try {
            producer.send(context.createTemporaryQueue(), UUID.randomUUID());
            fail("Should have thrown an exception");
        } catch (IllegalStateRuntimeException isre) {}
    }

    @Test
    public void testRuntimeExceptionFromSendStringBody() throws JMSException {
        JMSProducer producer = context.createProducer();

        MockJMSConnection connection = (MockJMSConnection) context.getConnection();
        connection.addConnectionListener(new MockJMSConnectionListener() {

            @Override
            public void onMessageSend(MockJMSSession session, Message message) throws JMSException {
                throw new IllegalStateException("Send Failed");
            }
        });

        try {
            producer.send(context.createTemporaryQueue(), "test");
            fail("Should have thrown an exception");
        } catch (IllegalStateRuntimeException isre) {}
    }

    @Test
    public void testRuntimeExceptionOnSendWithCompletion() throws JMSException {
        JMSProducer producer = context.createProducer();

        MockJMSConnection connection = (MockJMSConnection) context.getConnection();
        connection.addConnectionListener(new MockJMSConnectionListener() {

            @Override
            public void onMessageSend(MockJMSSession session, Message message) throws JMSException {
                throw new IllegalStateException("Send Failed");
            }
        });

        producer.setAsync(new CompletionListener() {

            @Override
            public void onException(Message message, Exception exception) {
            }

            @Override
            public void onCompletion(Message message) {
            }
        });

        try {
            producer.send(context.createTemporaryQueue(), "test");
            fail("Should have thrown an exception");
        } catch (IllegalStateRuntimeException isre) {}
    }

    //----- Internal Support -------------------------------------------------//

    private class TestJmsCompletionListener implements CompletionListener {

        @Override
        public void onCompletion(Message message) {
        }

        @Override
        public void onException(Message message, Exception exception) {
        }
    }
}
