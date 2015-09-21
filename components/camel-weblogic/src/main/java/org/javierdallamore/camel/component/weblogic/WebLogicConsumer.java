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
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.StartupListener;
import org.apache.camel.impl.DefaultConsumer;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
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
	private Queue q;
	private QueueConnection qc;
	private MBeanInfo bean;
	private JMXConnector jmxCon;
	private ObjectMapper mapper;
	private ObjectName mbeanName;
	private HashSet<String> attributes;

	public WebLogicConsumer(WebLogicEndpoint endpoint, Processor processor) {
		super(endpoint, processor);
		this.endpoint = endpoint;
		this.mapper = new ObjectMapper();
	}

	private ObjectNode generateJMSBody(List<Message> messages)
			throws JsonGenerationException, JsonMappingException, IOException,
			JMSException {
		ObjectNode msgRoot = mapper.createObjectNode();
		if (endpoint.isReadMessage()) {
			ArrayNode messagesNode = msgRoot.putArray("messages");
			for (Iterator<Message> iter = messages.iterator(); iter.hasNext();) {
				ObjectNode msgNode = generateMessageNode(mapper,
						(Message) iter.next());
				messagesNode.add(msgNode);
			}
		}
		return msgRoot;
	}

	private ObjectNode generateMessageNode(ObjectMapper mapper, Message message)
			throws JMSException {
		ObjectNode msgNode = mapper.createObjectNode();
		Enumeration<String> names = message.getPropertyNames();
		while (names.hasMoreElements()) {
			String name = (String) names.nextElement();
			Object value = message.getObjectProperty(name);
			if (value != null) {
				msgNode.put(name, value.toString());
			}
		}

		if (endpoint.isReadMessageBody()) {
			if (message instanceof TextMessage) {
				TextMessage txtMsg = (TextMessage) message;
				msgNode.put("body", txtMsg.getText());
			}
		}

		msgNode.put("correlationID", message.getJMSCorrelationID());
		msgNode.put("deliveryModeCode", message.getJMSDeliveryMode());
		if (message.getJMSDeliveryMode() == DeliveryMode.PERSISTENT) {
			msgNode.put("deliveryMode", "PERSISTENT");
		} else {
			msgNode.put("deliveryMode", "NON_PERSISTENT");
		}
		msgNode.put("expiration", message.getJMSExpiration());
		msgNode.put("messageID", message.getJMSMessageID());
		msgNode.put("priority", message.getJMSPriority());
		msgNode.put("redelivered", message.getJMSRedelivered());
		msgNode.put("timestamp", message.getJMSTimestamp());
		msgNode.put("type", message.getJMSType());
		msgNode.put("destination", "" + message.getJMSDestination());
		msgNode.put("replyTo", "" + message.getJMSReplyTo());
		return msgNode;
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
					ObjectNode root = mapper.createObjectNode();
					if (endpoint.isReadMessage()
							|| endpoint.isReadMessageBody()) {
						root.put("messages", processJMS());
					}
					if (endpoint.isReadMetrics()) {
						root.put("metrics", processJMX());
					}
					String body = mapper.writeValueAsString(root);

					Exchange exchange = endpoint.createExchange();
					exchange.getIn().setBody(body);
					try {
						getProcessor().process(exchange);
					} catch (Exception e) {
						getExceptionHandler().handleException(e);
					}
				} catch (Exception e) {
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
		if (!configured) {
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
		try {
			browser.close();
			qc.close();
			jmxCon.close();
		} catch (Exception ex) {
			LOG.error(ex.getMessage());
		}
	}

	protected void configureTask(TimerTask task, Timer timer) {
		try {
			if (this.endpoint.isReadMessageBody()
					|| this.endpoint.isReadMessage()) {
				initJMS();
			}
			if (this.endpoint.isReadMetrics()) {
				initJMX();
			}
			timer.scheduleAtFixedRate(task, endpoint.getDelay(),
					endpoint.getPeriod());
			configured = true;
		} catch (Exception e) {
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
		return isRunAllowed() && !isSuspended();
	}

	@Override
	public WebLogicEndpoint getEndpoint() {
		return (WebLogicEndpoint) super.getEndpoint();
	}

	private void initJMS() throws JMSException, NamingException {
		// create InitialContext
		Hashtable<String, String> properties = new Hashtable<String, String>();
		properties.put(Context.INITIAL_CONTEXT_FACTORY,
				"weblogic.jndi.WLInitialContextFactory");
		String url = "t3://" + this.endpoint.getHost();
		properties.put(Context.PROVIDER_URL, url);
		properties.put(Context.SECURITY_PRINCIPAL, this.endpoint.getUser());
		properties.put(Context.SECURITY_CREDENTIALS,
				this.endpoint.getPassword());

		InitialContext ctx;
		ctx = new InitialContext(properties);
		QueueConnectionFactory qcf = (QueueConnectionFactory) ctx
				.lookup(this.endpoint.getCf());
		q = (Queue) ctx.lookup(this.endpoint.getQueue());
		qc = qcf.createQueueConnection();
		qc.start();
	}

	private ObjectNode processJMS() throws JMSException,
			JsonGenerationException, JsonMappingException, IOException {
		qc.start();
		QueueSession qsess = qc.createQueueSession(false,
				Session.AUTO_ACKNOWLEDGE);
		browser = qsess.createBrowser(q);
		List<Message> messages = Collections.list(browser.getEnumeration());

		browser.close();
		qsess.close();
		qc.stop();
		return generateJMSBody(messages);
	}

	private void initJMX() throws IOException, InstanceNotFoundException,
			IntrospectionException, ReflectionException {
		String[] endpoint = this.endpoint.getHost().split(":");
		String hostname = endpoint[0];
		int port = Integer.parseInt(endpoint[1]);
		String protocol = "rmi";
		String jndiroot = new String("/jndi/iiop://" + hostname + ":" + port
				+ "/");
		String mserver = "weblogic.management.mbeanservers.runtime";
		JMXServiceURL jmxServiceURL = new JMXServiceURL(protocol, hostname, port, jndiroot
				+ mserver);
		Map<String, String> jmxCredentials = new Hashtable<String, String>();
		jmxCredentials.put(Context.SECURITY_PRINCIPAL, this.endpoint.getUser());
		jmxCredentials.put(Context.SECURITY_CREDENTIALS,
				this.endpoint.getPassword());
		attributes = new HashSet<String>();
		jmxCon = JMXConnectorFactory.connect(jmxServiceURL, jmxCredentials);

		MBeanServerConnection con = jmxCon.getMBeanServerConnection();
		if (this.mbeanName == null) {
			for (ObjectName objName : con.queryNames(null, null)) {
				if (objName.getCanonicalName().contains(
						"Name=" + this.endpoint.getDestinationName())) {
					this.mbeanName = objName;
					break;
				}
			}
		}

		if (attributes == null || attributes.size() == 0) {
			attributes = new HashSet<String>();
			this.bean = con.getMBeanInfo(this.mbeanName);
			if (this.bean == null) {
				throw new IOException("Destination Name " + this.mbeanName
						+ " does not exists in server " + this.endpoint.getHost());
			}
			for (MBeanAttributeInfo attr : this.bean.getAttributes()) {
				attributes.add(attr.getName());
			}
		}
	}

	private ObjectNode processJMX() throws IOException {
		ObjectNode metricsRoot = mapper.createObjectNode();
		try {
			MBeanServerConnection con = jmxCon.getMBeanServerConnection();
			for (String attrName : attributes) {
				Object attrValue = null;
				try {
					attrValue = con.getAttribute(this.mbeanName, attrName);
					if (attrValue != null) {
						metricsRoot.put(attrName, attrValue.toString());
					}
				} catch (Exception ex) {
					LOG.warn(attrName + ": " + attrValue + "\n" + ex.toString());
				}
			}
		} catch (Exception ex) {
			LOG.error(ex.getMessage(), ex);
		}
		return metricsRoot;
	}
}