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

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PooledConnectionExpiredConnectionsUnderLoad extends ActiveMQJmsPoolTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(PooledConnectionExpiredConnectionsUnderLoad.class);

    @Test(timeout = 120000)
    public void testExpiredConnectionsAreNotReturned() throws JMSException, InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicBoolean done = new AtomicBoolean(false);
        final JmsPoolConnectionFactory pooled = createPooledConnectionFactory();

        try {
            pooled.setMaxConnections(2);
            pooled.setExpiryTimeout(1L);
            Thread[] threads = new Thread[5];
            for (int i = 0; i < threads.length; i++) {
                threads[i] = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        while (!done.get() && latch.getCount() > 0) {
                            JmsPoolConnection pooledConnection = null;
                            try {
                                pooledConnection = (JmsPoolConnection) pooled.createConnection();
                                if (pooledConnection.getConnection() == null) {
                                    LOG.info("Found broken connection.");
                                    latch.countDown();
                                }
                            } catch (JMSException e) {
                                LOG.warn("Caught Exception", e);
                            } finally {
                                try {
                                    pooledConnection.close();
                                } catch (JMSException e) {
                                }
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
        } finally {
            pooled.stop();
        }
    }
}
