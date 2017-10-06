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

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.messaginghub.pooled.jms.JmsPoolConnectionFactory;
import org.messaginghub.pooled.jms.mock.MockJMSConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JmsPoolTestSupport {

    @Rule public TestName name = new TestName();

    protected static final Logger LOG = LoggerFactory.getLogger(JmsPoolTestSupport.class);

    protected MockJMSConnectionFactory factory;
    protected JmsPoolConnectionFactory cf;

    @Before
    public void setUp() throws Exception {
        LOG.info("========== start test: " + getTestName() + " ==========");

        factory = new MockJMSConnectionFactory();
        cf = new JmsPoolConnectionFactory();
        cf.setConnectionFactory(factory);
        cf.setMaxConnections(1);
    }

    @After
    public void tearDown() throws Exception {
        try {
            cf.stop();
        } catch (Exception ex) {
            // ignored
        }

        LOG.info("========== finished test " + getTestName() + " ==========");
    }

    public String getTestName() {
        return name.getMethodName();
    }
}