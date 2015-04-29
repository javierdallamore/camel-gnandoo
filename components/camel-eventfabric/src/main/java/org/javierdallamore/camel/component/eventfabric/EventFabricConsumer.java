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
package org.javierdallamore.camel.component.eventfabric;

import java.io.IOException;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.StartupListener;
import org.apache.camel.impl.DefaultConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.eventfabric.api.client.EventClient;
import com.eventfabric.api.client.Response;

/**
 * @version $Revision: 1.1 $
 */
public class EventFabricConsumer extends DefaultConsumer implements
		StartupListener {
	private static final Logger LOG = LoggerFactory
			.getLogger(EventFabricConsumer.class);
	private EventFabricEndpoint endpoint;
	private volatile boolean configured;

	public EventFabricConsumer(EventFabricEndpoint endpoint, Processor processor) {
		super(endpoint, processor);
		this.endpoint = endpoint;
	}

	@Override
	protected void doStart() throws Exception {
		super.doStart();
		if (!configured && endpoint.getCamelContext().getStatus().isStarted()) {
			configureTask();
		}
	}

	private void configureTask() {
		String channel = endpoint.getChannel();
		EventClient eventClient = endpoint.getEventClient();
		if (channel == null) {
			channel = endpoint.getName();
		}
		configured = true;
		while (isRunAllowed() && configured) {
			try {
				LOG.info(String.format("IsRunAllowed: %s. Is Configured: %s",
						isRunAllowed(), configured));
				LOG.info(String.format("Trying to listen to events: %s, %s",
						channel, endpoint.getBucket()));
				Response response = eventClient.listen(channel,
						endpoint.getBucket());
				LOG.info(String.format("Response: %s", response.getResult()));
				if (response.getStatus() == 200) {
					Exchange exchange = endpoint.createExchange();
					exchange.getIn().setBody(response.getResult());
					try {
						getProcessor().process(exchange);
					} catch (Exception e) {
						getExceptionHandler().handleException(e);
					}
				}
			} catch (IOException e) {
				LOG.error(e.getMessage());
			}
		}
	}

	/**
	 * Whether the timer task is allow to run or not
	 */
	protected boolean isTaskRunAllowed() {
		// only allow running the timer task if we can run and are not
		// suspended,
		// and CamelContext must have been fully started
		return endpoint.getCamelContext().getStatus().isStarted() &&
				getEndpoint().getCamelContext().getStatus().isStarted() &&
				!endpoint.getCamelContext().getStatus().isSuspended() && 
				!endpoint.getCamelContext().getStatus().isStopped() && 
				isRunAllowed() && !isSuspended();
	}

	@Override
	protected void doStop() throws Exception {
		LOG.info(String
				.format("Stopping Event Fabric Consumer. Wait for timeout"));
		configured = false;
		super.doStop();
	}                                                                        
                                                                               
    @Override                                                                  
    protected void doSuspend() throws Exception {
		LOG.info(String
				.format("Suspending Event Fabric Consumer. Wait for timeout"));
		configured = false;
    	super.doSuspend();                                       
    }

	public void onCamelContextStarted(CamelContext context,
			boolean alreadyStarted) throws Exception {
		if (!configured) {
			configureTask();
		}
	}
}
