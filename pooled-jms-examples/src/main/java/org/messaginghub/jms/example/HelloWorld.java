/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.messaginghub.jms.example;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.Context;
import javax.naming.InitialContext;

import org.messaginghub.pooled.jms.JmsPoolConnectionFactory;

public class HelloWorld {

    public static void main(String[] args) throws Exception {
        JmsPoolConnectionFactory poolingFactory = new JmsPoolConnectionFactory();

        try {
            // The configuration for the Qpid InitialContextFactory has been supplied in
            // a jndi.properties file in the classpath, which results in it being picked
            // up automatically by the InitialContext constructor.
            Context context = new InitialContext();

            ConnectionFactory factory = (ConnectionFactory) context.lookup("myFactoryLookup");

            poolingFactory.setConnectionFactory(factory);

            Destination queue = (Destination) context.lookup("myQueueLookup");

            final String messagePayload = "Hello World";

            // Each send should end up reusing the same Connection and Session from the pool
            for (int i = 0; i < messagePayload.length(); ++i) {
                Connection connection = poolingFactory.createConnection(System.getProperty("USER"), System.getProperty("PASSWORD"));
                connection.setExceptionListener(new MyExceptionListener());

                Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

                MessageProducer messageProducer = session.createProducer(queue);

                TextMessage message = session.createTextMessage("" + messagePayload.charAt(i));
                messageProducer.send(message, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);

                connection.close();
            }

            // The consumer should reuse the same Connection as the senders, broker should register only
            // one connection having been used for this full example.
            Connection connection = poolingFactory.createConnection(System.getProperty("USER"), System.getProperty("PASSWORD"));
            connection.setExceptionListener(new MyExceptionListener());
            connection.start();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            MessageConsumer messageConsumer = session.createConsumer(queue);
            for (int i = 0; i < messagePayload.length(); ++i) {
                TextMessage receivedMessage = (TextMessage) messageConsumer.receive(2000l);
                if (receivedMessage != null) {
                    System.out.print(receivedMessage.getText());
                } else {
                    System.out.println("No message received within the given timeout!");
                }
            }

            System.out.println();

            connection.close();
        } catch (Exception exp) {
            System.out.println("Caught exception, exiting.");
            exp.printStackTrace(System.out);
            System.exit(1);
        } finally {
            poolingFactory.stop();
        }
    }

    private static class MyExceptionListener implements ExceptionListener {
        @Override
        public void onException(JMSException exception) {
            System.out.println("Connection ExceptionListener fired, exiting.");
            exception.printStackTrace(System.out);
            System.exit(1);
        }
    }
}
