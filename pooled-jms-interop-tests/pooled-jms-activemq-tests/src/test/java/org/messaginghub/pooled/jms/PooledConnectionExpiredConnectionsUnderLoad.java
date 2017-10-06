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

import static org.junit.Assert.fail;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.jms.JMSException;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.junit.Before;
import org.junit.Test;
import org.messaginghub.pooled.jms.JmsPoolConnection;
import org.messaginghub.pooled.jms.JmsPoolConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PooledConnectionExpiredConnectionsUnderLoad extends ActiveMQJmsPoolTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(PooledConnectionExpiredConnectionsUnderLoad.class);

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        brokerService = new BrokerService();
        brokerService.setDeleteAllMessagesOnStartup(true);
        brokerService.setPersistent(false);
        brokerService.setUseJmx(false);
        brokerService.setAdvisorySupport(false);
        brokerService.setSchedulerSupport(false);
        brokerService.start();
        brokerService.waitUntilStarted();
    }

    @Test(timeout = 120000)
    public void testExpiredConnectionsAreNotReturned() throws JMSException, InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicBoolean done = new AtomicBoolean(false);
        final JmsPoolConnectionFactory pooled = new JmsPoolConnectionFactory();
        pooled.setConnectionFactory(new ActiveMQConnectionFactory("vm://localhost?create=false"));

        pooled.setMaxConnections(2);
        pooled.setExpiryTimeout(1L);
        Thread[] threads = new Thread[5];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(new Runnable() {
                @Override
                public void run() {
                    while (!done.get() && latch.getCount() > 0) {
                        try {
                            final JmsPoolConnection pooledConnection = (JmsPoolConnection) pooled.createConnection();
                            if (pooledConnection.getConnection() == null) {
                                LOG.info("Found broken connection.");
                                latch.countDown();
                            }
                            pooledConnection.close();
                        } catch (JMSException e) {
                            LOG.warn("Caught Exception", e);
                        }
                    }
                }
            });
        }

        for (Thread thread : threads) {
            thread.start();
        }

        if (latch.await(3, TimeUnit.SECONDS)) {
            fail("A thread obtained broken connection");
        }

        done.set(true);

        for (Thread thread : threads) {
            thread.join();
        }
    }
}
