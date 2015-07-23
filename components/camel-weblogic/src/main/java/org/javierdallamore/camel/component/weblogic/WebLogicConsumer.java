/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.javierdallamore.camel.component.weblogic;

import java.io.IOException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLong;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.StartupListener;
import org.apache.camel.component.timer.TimerEndpoint;
import org.apache.camel.impl.DefaultConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @version $Revision: 1.1 $
 */
public class WebLogicConsumer extends DefaultConsumer implements
		StartupListener {
	private static final Logger LOG = LoggerFactory
			.getLogger(WebLogicConsumer.class);
	private WebLogicEndpoint endpoint;
	private volatile TimerTask task;
	private volatile boolean configured;
	private QueueBrowser browser;

	public WebLogicConsumer(WebLogicEndpoint endpoint, Processor processor) {
		super(endpoint, processor);
		this.endpoint = endpoint;
	}

	@Override
	protected void doStart() throws Exception {
		task = new TimerTask() {
			@Override
			public void run() {
				if (!isTaskRunAllowed()) {
					// do not run timer task as it was not allowed
					LOG.debug("Run now allowed for timer: {}", endpoint);
					return;
				}
				try {
					List<Message> messages = Collections.list(browser
							.getEnumeration());
					for (Iterator<Message> iter = messages.iterator(); iter
							.hasNext();) {
						Message tempMsg = (Message) iter.next();
						System.out.println("Message" + tempMsg);

						Enumeration names = tempMsg.getPropertyNames();
						while (names.hasMoreElements()) {
							String name = "" + names.nextElement();
							System.out.println(name + ": "
									+ tempMsg.getObjectProperty(name));
						}

						if (tempMsg instanceof TextMessage) {
							TextMessage txtMsg = (TextMessage) tempMsg;
							Exchange exchange = endpoint.createExchange();
							exchange.getIn().setBody(txtMsg.getText());
							try {
								getProcessor().process(exchange);
							} catch (Exception e) {
								getExceptionHandler().handleException(e);
							}
						}
					}
				} catch (JMSException e) {
					LOG.error(e.getMessage());
					LOG.warn(
							"Error processing exchange. This exception will be ignored, to let the timer be able to trigger again.",
							e);
				}

			}
		};
		// only configure task if CamelContext already started, otherwise the
		// StartupListener
		// is configuring the task later
		if (!configured && endpoint.getCamelContext().getStatus().isStarted()) {
			Timer timer = endpoint.getTimer(this);
			configureTask(task, timer);
		}
	}

	@Override
	protected void doStop() throws Exception {
		if (task != null) {
			task.cancel();
		}
		task = null;
		configured = false;

		// remove timer
		endpoint.removeTimer(this);
	}

	protected void configureTask(TimerTask task, Timer timer) {
		try {
			init();
			timer.scheduleAtFixedRate(task, endpoint.getDelay(),
					endpoint.getPeriod());
			configured = true;
		} catch (JMSException e) {
			LOG.error(e.getMessage());
		} catch (NamingException e) {
			LOG.error(e.getMessage());
		}
	}

	@Override
	public void onCamelContextStarted(CamelContext context,
			boolean alreadyStarted) throws Exception {
		if (task != null && !configured) {
			Timer timer = endpoint.getTimer(this);
			configureTask(task, timer);
		}
	}

	/**
	 * Whether the timer task is allow to run or not
	 */
	protected boolean isTaskRunAllowed() {
		// only allow running the timer task if we can run and are not
		// suspended,
		// and CamelContext must have been fully started
		return endpoint.getCamelContext().getStatus().isStarted()
				&& isRunAllowed() && !isSuspended();
	}

	@Override
	public WebLogicEndpoint getEndpoint() {
		return (WebLogicEndpoint) super.getEndpoint();
	}

	private void init() throws JMSException, NamingException {
		// create InitialContext
		Hashtable<String, String> properties = new Hashtable<String, String>();
		properties.put(Context.INITIAL_CONTEXT_FACTORY,
				"weblogic.jndi.WLInitialContextFactory");
		properties.put(Context.PROVIDER_URL, this.endpoint.getUrl());
		properties.put(Context.SECURITY_PRINCIPAL, this.endpoint.getUser());
		properties.put(Context.SECURITY_CREDENTIALS,
				this.endpoint.getPassword());

		InitialContext ctx;

		ctx = new InitialContext(properties);

		System.out.println("Got InitialContext " + ctx.toString());

		QueueConnectionFactory qcf = (QueueConnectionFactory) ctx
				.lookup(this.endpoint.getCf());
		System.out.println("Got QueueConnectionFactory " + qcf.toString());

		QueueConnection qc = qcf.createQueueConnection();
		System.out.println("Got QueueConnection " + qc.toString());

		QueueSession qsess = qc.createQueueSession(false,
				Session.AUTO_ACKNOWLEDGE);
		System.out.println("Got QueueSession " + qsess.toString());

		Queue q = (Queue) ctx.lookup(this.endpoint.getQueue());
		System.out.println("Got Queue " + q.toString());

		browser = qsess.createBrowser(q);
		System.out.println("Got Browser " + browser.toString());

		qc.start();
	}
}
