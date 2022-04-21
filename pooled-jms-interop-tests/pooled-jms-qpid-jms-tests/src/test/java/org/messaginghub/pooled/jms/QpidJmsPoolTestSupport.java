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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyAcceptorFactory;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.embedded.EmbeddedActiveMQ;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.spi.core.remoting.Acceptor;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.messaginghub.pooled.jms.util.Wait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QpidJmsPoolTestSupport {

    protected static final Logger LOG = LoggerFactory.getLogger(QpidJmsPoolTestSupport.class);

    private static final String NETTY_ACCEPTOR = "netty-acceptor";
    private static final String SERVER_NAME = "embedded-server";

    protected String testName;
    protected String connectionURI;
    protected JmsConnectionFactory qpidJmsConnectionFactory;

    protected Configuration configuration;
    protected EmbeddedActiveMQ broker;

    @BeforeEach
    public void setUp(TestInfo info) throws Exception {
        LOG.info("========== start " + info.getDisplayName() + " ==========");

        testName = info.getDisplayName();
        broker = createBroker();

        qpidJmsConnectionFactory = new JmsConnectionFactory(connectionURI);
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (broker != null) {
            try {
                broker.stop();
            } catch (Exception ex) {
                LOG.warn("Suppress error on shutdown: {}", ex);
            }
        }

        LOG.info("========== tearDown " + getTestName() + " ==========");
    }

    public String getTestName() {
        return testName;
    }

    protected EmbeddedActiveMQ createBroker() throws Exception {
        Map<String, Object> params = new HashMap<>();
        params.put(TransportConstants.PORT_PROP_NAME, 0);
        params.put(TransportConstants.PROTOCOLS_PROP_NAME, "AMQP");

        TransportConfiguration tc = new TransportConfiguration(NettyAcceptorFactory.class.getName(), params, NETTY_ACCEPTOR);

        configuration = new ConfigurationImpl().setName(SERVER_NAME)
                                                 .setPersistenceEnabled(false)
                                                 .setSecurityEnabled(false)
                                                 .addAcceptorConfiguration(tc)
                                                 .addAddressesSetting("#",
                                                         new AddressSettings().setDeadLetterAddress(SimpleString.toSimpleString("dla"))
                                                         .setExpiryAddress(SimpleString.toSimpleString("expiry")));
        broker = new EmbeddedActiveMQ().setConfiguration(configuration);
        broker.start();

        ActiveMQServer server = broker.getActiveMQServer();
        if(!server.waitForActivation(5000, TimeUnit.MILLISECONDS)) {
            throw new IllegalStateException("Server not activated within timeout");
        }

        Acceptor acceptor = server.getRemotingService().getAcceptor(NETTY_ACCEPTOR);
        if(!Wait.waitFor(() -> acceptor.getActualPort() > 0, 5000, 20)) {
            throw new IllegalStateException("Acceptor port not determined within timeout");
        }
        int actualPort = acceptor.getActualPort();

        LOG.error("Acceptor was bound to port " + actualPort);//TODO: level

        connectionURI = "amqp://localhost:" + actualPort;

        return broker;
    }

    protected JmsPoolConnectionFactory createPooledConnectionFactory() {
        JmsPoolConnectionFactory cf = new JmsPoolConnectionFactory();
        cf.setConnectionFactory(qpidJmsConnectionFactory);
        cf.setMaxConnections(1);
        LOG.debug("ConnectionFactory initialized.");
        return cf;
    }
}
