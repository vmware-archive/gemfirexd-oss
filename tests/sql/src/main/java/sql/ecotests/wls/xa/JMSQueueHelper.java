/*
 * Copyright (c) 2010-2015 Pivotal Software, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package sql.ecotests.wls.xa;

import javax.jms.*;
import javax.naming.Context;
import javax.naming.NamingException;

public class JMSQueueHelper {
    private Context ictx;
    private String queueConnFactoryName;
    private String queueName;
    private QueueConnectionFactory qconFactory;
    private QueueConnection qSenderConn;
    private QueueConnection qReceiverConn;
    private QueueSession qSenderSession;
    private QueueSession qReceiverSession;
    private QueueSender qSender;
    private QueueReceiver qReceiver;
    private Queue queue;
    private TextMessage msg;

    public JMSQueueHelper(Context ictx, String queueConnFactoryName, String queueName) {
        this.ictx = ictx;
        this.queueConnFactoryName = queueConnFactoryName;
        this.queueName = queueName;
    }

    public void init() throws NamingException, JMSException {
        qconFactory = (QueueConnectionFactory) ictx.lookup(queueConnFactoryName);
        queue = (Queue) ictx.lookup(queueName);
    }

    public void send(String message) throws JMSException {
		qSenderConn = qconFactory.createQueueConnection();
		qSenderSession = qSenderConn.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
		qSender = qSenderSession.createSender(queue);
		msg = qSenderSession.createTextMessage();
        qSenderConn.start();
 		msg.setText(message);
        qSender.send(msg);
    }

    public String receive() throws JMSException {
        String msgText = null;
        qReceiverConn = qconFactory.createQueueConnection();
        qReceiverSession = qReceiverConn.createQueueSession(false, javax.jms.Session.AUTO_ACKNOWLEDGE);
        qReceiver = qReceiverSession.createReceiver(queue);
        qReceiverConn.start();

		Message rmsg = qReceiver.receiveNoWait();
		if (rmsg != null) {
			if (rmsg instanceof TextMessage) {
				msgText = ((TextMessage) rmsg).getText();
			} else {
				msgText = rmsg.toString();
			}
		}

        return msgText;
    }

    public void closeSender() {
		if (qSender != null) {
			try {
				qSender.close();
			} catch (JMSException ex) {
			}
		}
		if (qSenderSession != null) {
			try {
				qSenderSession.close();
			} catch (JMSException ex) {
			}
		}
		if (qSenderConn != null) {
			try {
				qSenderConn.close();
			} catch (JMSException ex) {
			}
		}
    }

    public void closeReceiver() {
		if (qReceiver != null) {
			try {
				qReceiver.close();
			} catch (JMSException ex) {
			}
		}
		if (qReceiverSession != null) {
			try {
				qReceiverSession.close();
			} catch (JMSException ex) {
			}
		}
		if (qReceiverConn != null) {
			try {
				qReceiverConn.close();
			} catch (JMSException ex) {
			}
		}
	}
}