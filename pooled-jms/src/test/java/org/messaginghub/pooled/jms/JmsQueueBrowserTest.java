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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import jakarta.jms.IllegalStateException;
import jakarta.jms.JMSException;
import jakarta.jms.Queue;
import jakarta.jms.QueueBrowser;
import jakarta.jms.QueueSession;
import jakarta.jms.Session;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Tests for the JMS Pool QueueBrowser wrapper
 */
@Timeout(60)
public class JmsQueueBrowserTest extends JmsPoolTestSupport {

    @Test
    public void testToString() throws JMSException {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createQueueConnection();
        QueueSession session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createTemporaryQueue();
        QueueBrowser browser = session.createBrowser(queue);

        assertNotNull(browser.toString());
    }

    @Test
    public void testGetQueue() throws JMSException {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createQueueConnection();
        QueueSession session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createTemporaryQueue();
        QueueBrowser browser = session.createBrowser(queue);

        assertNotNull(browser.getQueue());

        browser.close();
        browser.close();

        try {
            browser.getQueue();
            fail("Should not be able to use a closed browser");
        } catch (IllegalStateException ise) {
        }
    }

    @Test
    public void testGetQueueBrowser() throws JMSException {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createQueueConnection();
        QueueSession session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createTemporaryQueue();
        JmsPoolQueueBrowser browser = (JmsPoolQueueBrowser) session.createBrowser(queue);

        assertNotNull(browser.getQueueBrowser());

        browser.close();

        try {
            browser.getQueueBrowser();
            fail("Should not be able to use a closed browser");
        } catch (IllegalStateException ise) {
        }
    }

    @Test
    public void testGetMessageSelector() throws JMSException {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createQueueConnection();
        QueueSession session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createTemporaryQueue();
        QueueBrowser browser = session.createBrowser(queue, "color = red");

        assertNotNull(browser.getMessageSelector());
        assertEquals("color = red", browser.getMessageSelector());

        browser.close();

        try {
            browser.getMessageSelector();
            fail("Should not be able to use a closed browser");
        } catch (IllegalStateException ise) {
        }
    }

    @Test
    public void testGetEnumeration() throws JMSException {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createQueueConnection();
        QueueSession session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createTemporaryQueue();
        QueueBrowser browser = session.createBrowser(queue);

        assertNotNull(browser.getEnumeration());

        browser.close();

        try {
            browser.getEnumeration();
            fail("Should not be able to use a closed browser");
        } catch (IllegalStateException ise) {
        }
    }
}
