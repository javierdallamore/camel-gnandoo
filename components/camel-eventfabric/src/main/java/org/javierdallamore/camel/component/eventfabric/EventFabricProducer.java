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

import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultProducer;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

import com.eventfabric.api.client.EndPointInfo;
import com.eventfabric.api.client.EventClient;
import com.eventfabric.api.client.Response;
import com.eventfabric.api.model.Event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @version $Revision: 1.1 $
 */
public class EventFabricProducer extends DefaultProducer {
	private static final Logger LOG = LoggerFactory
			.getLogger(EventFabricProducer.class);
	private final EventFabricEndpoint endpoint;
	private ObjectMapper mapper = new ObjectMapper();
	
	public EventFabricProducer(EventFabricEndpoint endpoint) {
		super(endpoint);
		this.endpoint = endpoint;
	}

	/**
	 *
	 * @param exchange
	 * @throws Exception
	 */
	@Override
	public void process(Exchange exchange) throws Exception {
		String body = exchange.getIn().getBody(String.class);
		body = String.format("{\"data\": %s}", body);
		ObjectNode value = (ObjectNode) mapper.readTree(body);
		String channel = endpoint.getChannel();
		if (channel == null) {
			channel = endpoint.getName();
		}
		try {
			if (endpoint.isAuthenticated()) {
				Event event = new Event(channel, value);
				Response response = endpoint.getEventClient().send(event);
				if (response.getStatus() == 201) {
					System.out.println(String.format("%s sent to Event Fabric", endpoint.getName()));
				} else {
					System.out.println(String.format("Error sending %s to Event Fabric: %s", endpoint.getName(), response.getResult()));
				}
			} else {
				System.out.println("Error logging in Event Fabric");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
