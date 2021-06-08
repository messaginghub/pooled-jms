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

import java.util.Map;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.remoting.impl.invm.InVMAcceptorFactory;
import org.apache.activemq.artemis.core.remoting.impl.invm.TransportConstants;
import org.apache.activemq.artemis.core.server.embedded.EmbeddedActiveMQ;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ArtemisJmsPoolTestSupport {

    protected static final Logger LOG = LoggerFactory.getLogger(ArtemisJmsPoolTestSupport.class);

    private static final String SERVER_NAME = "embedded-server";

    protected String testName;
    protected ActiveMQConnectionFactory artemisJmsConnectionFactory;
    protected JmsPoolConnectionFactory cf;

    protected Configuration configuration;
    protected EmbeddedActiveMQ server;

    @BeforeEach
    public void setUp(TestInfo info) throws Exception {
        LOG.info("========== started test: " + info.getDisplayName() + " ==========");

        testName = info.getDisplayName();

        configuration = new ConfigurationImpl().setName(SERVER_NAME)
                                               .setPersistenceEnabled(false)
                                               .setSecurityEnabled(false)
                                               .addAcceptorConfiguration(new TransportConfiguration(InVMAcceptorFactory.class.getName()))
                                               .addAddressesSetting("#", new AddressSettings().setDeadLetterAddress(SimpleString.toSimpleString("dla")).setExpiryAddress(SimpleString.toSimpleString("expiry")));
        server = new EmbeddedActiveMQ().setConfiguration(configuration);
        server.start();

        artemisJmsConnectionFactory = new ActiveMQConnectionFactory(getVmURL());

        cf = new JmsPoolConnectionFactory();
        cf.setConnectionFactory(artemisJmsConnectionFactory);
        cf.setMaxConnections(1);
    }

    private String getVmURL() {
        String vmURL = "vm://0";
        for (TransportConfiguration transportConfiguration : configuration.getAcceptorConfigurations()) {
           Map<String, Object> params = transportConfiguration.getParams();
           if (params != null && params.containsKey(TransportConstants.SERVER_ID_PROP_NAME)) {
              vmURL = "vm://" + params.get(TransportConstants.SERVER_ID_PROP_NAME);
           }
        }

        return vmURL;
     }

    @AfterEach
    public void tearDown() throws Exception {
        if (cf != null) {
            cf.stop();
        }

        server.stop();

        LOG.info("========== completed test: " + getTestName() + " ==========");
    }

    public String getTestName() {
        return testName;
    }
}