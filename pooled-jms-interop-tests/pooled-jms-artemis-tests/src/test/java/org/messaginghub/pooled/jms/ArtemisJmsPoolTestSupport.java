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

import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.junit.EmbeddedJMSResource;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.messaginghub.pooled.jms.JmsPoolConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ArtemisJmsPoolTestSupport {

    @Rule public TestName name = new TestName();
    @Rule public EmbeddedJMSResource server = new EmbeddedJMSResource(false);

    protected static final Logger LOG = LoggerFactory.getLogger(ArtemisJmsPoolTestSupport.class);

    protected ActiveMQConnectionFactory artemisJmsConnectionFactory;
    protected JmsPoolConnectionFactory cf;

    @Before
    public void setUp() throws Exception {
        LOG.info("========== started test: " + getTestName() + " ==========");

        artemisJmsConnectionFactory = new ActiveMQConnectionFactory(server.getVmURL());

        cf = new JmsPoolConnectionFactory();
        cf.setConnectionFactory(artemisJmsConnectionFactory);
        cf.setMaxConnections(1);
        cf.setCreateConnectionOnStartup(false);
    }

    @After
    public void tearDown() throws Exception {
        if (cf != null) {
            cf.stop();
        }

        LOG.info("========== completed test: " + getTestName() + " ==========");
    }

    public String getTestName() {
        return name.getMethodName();
    }
}