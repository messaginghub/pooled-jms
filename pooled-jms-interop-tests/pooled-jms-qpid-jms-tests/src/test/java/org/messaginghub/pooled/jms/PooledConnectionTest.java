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

import static org.junit.jupiter.api.Assertions.fail;

import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.IllegalStateException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A test against the PooledConnection class using QpidJMS
 */
@Timeout(60)
public class PooledConnectionTest extends QpidJmsPoolTestSupport {

    private final Logger LOG = LoggerFactory.getLogger(PooledConnectionTest.class);

    @Test
    public void testSetClientIDTwiceWithSameID() throws Exception {
        // test: call setClientID("newID") twice
        // this should be tolerated and not result in an exception
        ConnectionFactory cf = createPooledConnectionFactory();
        Connection conn = cf.createConnection();
        conn.setClientID("newID");

        try {
            conn.setClientID("newID");
            conn.start();
            conn.close();
        } catch (IllegalStateException ise) {
            LOG.error("Repeated calls to newID2.setClientID(\"newID\") caused " + ise.getMessage());
            fail("Repeated calls to newID2.setClientID(\"newID\") caused " + ise.getMessage());
        } finally {
            ((JmsPoolConnectionFactory) cf).stop();
        }

        LOG.debug("Test finished.");
    }

    @Test
    public void testSetClientIDTwiceWithDifferentID() throws Exception {
        ConnectionFactory cf = createPooledConnectionFactory();
        Connection conn = cf.createConnection();

        // test: call setClientID() twice with different IDs
        // this should result in an IllegalStateException
        conn.setClientID("newID1");
        try {
            conn.setClientID("newID2");
            fail("calling Connection.setClientID() twice with different clientID must raise an IllegalStateException");
        } catch (IllegalStateException ise) {
            LOG.debug("Correctly received " + ise);
        } finally {
            conn.close();
            ((JmsPoolConnectionFactory) cf).stop();
        }

        LOG.debug("Test finished.");
    }

    @Test
    public void testSetClientIDAfterConnectionStart() throws Exception {
        ConnectionFactory cf = createPooledConnectionFactory();
        Connection conn = cf.createConnection();

        // test: try to call setClientID() after start()
        // should result in an exception
        try {
            conn.start();
            conn.setClientID("newID3");
            fail("Calling setClientID() after start() must raise a JMSException.");
        } catch (IllegalStateException ise) {
            LOG.debug("Correctly received " + ise);
        } finally {
            conn.close();
            ((JmsPoolConnectionFactory) cf).stop();
        }

        LOG.debug("Test finished.");
    }
}
