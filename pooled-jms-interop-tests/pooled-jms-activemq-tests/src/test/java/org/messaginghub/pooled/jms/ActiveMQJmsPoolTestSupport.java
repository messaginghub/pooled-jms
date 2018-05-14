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

import java.util.ArrayList;
import java.util.Set;

import javax.jms.JMSException;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQXAConnectionFactory;
import org.apache.activemq.advisory.AdvisorySupport;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.jmx.BrokerViewMBean;
import org.apache.activemq.broker.jmx.ConnectorViewMBean;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ConsumerInfo;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ActiveMQJmsPoolTestSupport {

    @Rule public TestName name = new TestName();

    protected static final Logger LOG = LoggerFactory.getLogger(ActiveMQJmsPoolTestSupport.class);

    protected BrokerService brokerService;
    protected String connectionURI;
    protected ActiveMQConnectionFactory amqFactory;
    protected ActiveMQXAConnectionFactory amqXAFactory;
    protected ActiveMQConnectionFactory amqFailoverFactory;

    @Before
    public void setUp() throws Exception {
        LOG.info("========== start " + getTestName() + " ==========");

        connectionURI = createBroker();
        amqFactory = new ActiveMQConnectionFactory(connectionURI);
        amqXAFactory = new ActiveMQXAConnectionFactory(connectionURI);
        amqFailoverFactory = new ActiveMQConnectionFactory("failover:" + connectionURI);
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

    protected String createBroker() throws Exception {
        brokerService = new BrokerService();
        brokerService.setDeleteAllMessagesOnStartup(true);
        brokerService.setPersistent(false);
        brokerService.setUseJmx(true);
        brokerService.getManagementContext().setCreateConnector(false);
        brokerService.getManagementContext().setCreateMBeanServer(false);
        brokerService.setAdvisorySupport(false);
        brokerService.setSchedulerSupport(false);
        brokerService.addConnector("tcp://localhost:0").setName("test");

        ArrayList<BrokerPlugin> plugins = new ArrayList<BrokerPlugin>();

        plugins.add(new BrokerPlugin() {

            @Override
            public Broker installPlugin(Broker broker) throws Exception {
                return new BrokerFilter(broker) {

                    @Override
                    public void addConnection(ConnectionContext context, ConnectionInfo info) throws Exception {
                        if ("invalid".equals(info.getUserName())) {
                            throw new SecurityException("Username or password was invalid");
                        }

                        super.addConnection(context, info);
                    }

                    @Override
                    public Subscription addConsumer(ConnectionContext context, ConsumerInfo info) throws Exception {
                        ActiveMQDestination destination = info.getDestination();

                        if (!AdvisorySupport.isAdvisoryTopic(destination) &&
                            !destination.getPhysicalName().startsWith("GUESTS")) {

                            if ("guest".equals(context.getUserName())) {
                                throw new SecurityException("User guest not authorized to read");
                            }
                        }

                        return super.addConsumer(context, info);
                    }
                };
            }
        });

        BrokerPlugin[] array = new BrokerPlugin[plugins.size()];

        brokerService.setPlugins(plugins.toArray(array));
        brokerService.start();
        brokerService.waitUntilStarted();

        return brokerService.getTransportConnectorByName("test").getPublishableConnectString();
    }

    protected JmsPoolConnectionFactory createPooledConnectionFactory() {
        JmsPoolConnectionFactory cf = new JmsPoolConnectionFactory();
        cf.setConnectionFactory(amqFactory);
        cf.setMaxConnections(1);
        LOG.debug("ConnectionFactory initialized.");
        return cf;
    }

    protected JmsPoolConnectionFactory createFailoverPooledConnectionFactory() {
        JmsPoolConnectionFactory cf = new JmsPoolConnectionFactory();
        cf.setConnectionFactory(amqFailoverFactory);
        cf.setMaxConnections(1);
        LOG.debug("Failover ConnectionFactory initialized.");
        return cf;
    }

    protected JmsPoolXAConnectionFactory createXAPooledConnectionFactory() {
        JmsPoolXAConnectionFactory cf = new JmsPoolXAConnectionFactory();
        cf.setConnectionFactory(amqXAFactory);
        cf.setMaxConnections(1);
        LOG.debug("Failover ConnectionFactory initialized.");
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