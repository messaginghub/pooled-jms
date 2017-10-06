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

import java.util.Set;

import javax.jms.JMSException;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.BrokerViewMBean;
import org.apache.activemq.broker.jmx.ConnectorViewMBean;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.messaginghub.pooled.jms.JmsPoolConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QpidJmsPoolTestSupport {

    @Rule public TestName name = new TestName();

    protected static final Logger LOG = LoggerFactory.getLogger(QpidJmsPoolTestSupport.class);

    protected BrokerService brokerService;
    protected String connectionURI;
    protected JmsConnectionFactory qpidJmsConnectionFactory;

    @Before
    public void setUp() throws Exception {
        LOG.info("========== start " + getTestName() + " ==========");

        brokerService = createBroker();

        qpidJmsConnectionFactory = new JmsConnectionFactory(connectionURI);
    }

    @After
    public void tearDown() throws Exception {
        if (brokerService != null) {
            try {
                brokerService.stop();
                brokerService.waitUntilStopped();
                brokerService = null;
            } catch (Exception ex) {
                LOG.warn("Suppress error on shutdown: {}", ex);
            }
        }

        LOG.info("========== tearDown " + getTestName() + " ==========");
    }

    public String getTestName() {
        return name.getMethodName();
    }

    protected BrokerService createBroker() throws Exception {
        BrokerService brokerService = new BrokerService();
        brokerService.setDeleteAllMessagesOnStartup(true);
        brokerService.setPersistent(false);
        brokerService.setUseJmx(false);
        brokerService.setAdvisorySupport(false);
        brokerService.setSchedulerSupport(false);

        connectionURI = brokerService.addConnector("amqp://localhost:0").getPublishableConnectString();

        brokerService.start();
        brokerService.waitUntilStarted();

        return brokerService;
    }

    protected JmsPoolConnectionFactory createPooledConnectionFactory() {
        JmsPoolConnectionFactory cf = new JmsPoolConnectionFactory();
        cf.setConnectionFactory(qpidJmsConnectionFactory);
        cf.setMaxConnections(1);
        LOG.debug("ConnectionFactory initialized.");
        return cf;
    }

    protected BrokerViewMBean getProxyToBroker() throws MalformedObjectNameException, JMSException {
        ObjectName brokerViewMBean = new ObjectName(
            "org.apache.activemq:type=Broker,brokerName=" + brokerService.getBrokerName());
        BrokerViewMBean proxy = (BrokerViewMBean) brokerService.getManagementContext()
                .newProxyInstance(brokerViewMBean, BrokerViewMBean.class, true);
        return proxy;
    }

    protected ConnectorViewMBean getProxyToConnectionView(String connectionType) throws Exception {
        ObjectName connectorQuery = new ObjectName(
            "org.apache.activemq:type=Broker,brokerName=" + brokerService.getBrokerName() + ",connector=clientConnectors,connectorName="+connectionType+"_//*");

        Set<ObjectName> results = brokerService.getManagementContext().queryNames(connectorQuery, null);

        if (results == null || results.isEmpty() || results.size() > 1) {
            throw new Exception("Unable to find the exact Connector instance.");
        }

        ConnectorViewMBean proxy = (ConnectorViewMBean) brokerService.getManagementContext()
                .newProxyInstance(results.iterator().next(), ConnectorViewMBean.class, true);
        return proxy;
    }

    protected QueueViewMBean getProxyToQueue(String name) throws MalformedObjectNameException, JMSException {
        ObjectName queueViewMBeanName = new ObjectName("org.apache.activemq:type=Broker,brokerName=" + brokerService.getBrokerName() + ",destinationType=Queue,destinationName="+name);
        QueueViewMBean proxy = (QueueViewMBean) brokerService.getManagementContext()
                .newProxyInstance(queueViewMBeanName, QueueViewMBean.class, true);
        return proxy;
    }

    protected QueueViewMBean getProxyToTopic(String name) throws MalformedObjectNameException, JMSException {
        ObjectName queueViewMBeanName = new ObjectName("org.apache.activemq:type=Broker,brokerName=" + brokerService.getBrokerName() + ",destinationType=Topic,destinationName="+name);
        QueueViewMBean proxy = (QueueViewMBean) brokerService.getManagementContext()
                .newProxyInstance(queueViewMBeanName, QueueViewMBean.class, true);
        return proxy;
    }
}