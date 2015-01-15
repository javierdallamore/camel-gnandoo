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
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;

import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultProducer;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

import com.eventfabric.api.client.EndPointInfo;
import com.eventfabric.api.client.EventClient;
import com.eventfabric.api.client.Response;
import com.eventfabric.api.model.Event;
import com.sun.xml.fastinfoset.Encoder;

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
	private int attemps = 0;
	
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
		attemps += 1;
		String data;
		if (endpoint.getInputEncoding() != null && !endpoint.getInputEncoding().isEmpty()) {
			byte[] body = exchange.getIn().getBody(byte[].class);
			Charset utf8charset = Charset.forName("UTF-8");
			Charset inputcharset = Charset.forName(endpoint.getInputEncoding());
			ByteBuffer inputBuffer = ByteBuffer.wrap(body);
			CharBuffer charBuffer = inputcharset.decode(inputBuffer);
			ByteBuffer outputBuffer = utf8charset.encode(charBuffer);
			byte[] outputData = outputBuffer.array();
			data = new String(outputData, "UTF-8");
		} else {
			data = exchange.getIn().getBody(String.class);
		}
		
		ObjectNode jsonNode;
		
		Object result = mapper.readTree(data);
		
		if (result instanceof ArrayNode) {
			jsonNode = mapper.createObjectNode();
			ArrayNode node = jsonNode.putArray("data");
			node.addAll((ArrayNode)result);
		} else {
			jsonNode = (ObjectNode)result;
		}
		
		String channel = endpoint.getChannel();
		EventClient eventClient = endpoint.getEventClient();
		if (channel == null) {
			channel = endpoint.getName();
		}
		try {
			Event event = new Event(channel, jsonNode);
			Response response = eventClient.send(event);
			if (response.getStatus() == 201) {
				LOG.info(String.format("%s sent to Event Fabric", endpoint.getName()));
			} else if (response.getStatus() == 401 && attemps <= 3) {
				LOG.error(String.format("Event Fabric session expired. Trying to log in again. Attemp: %d", attemps));
				eventClient.authenticate();
				process(exchange);
			} else {
				LOG.error(String.format("Error sending %s to Event Fabric: %s", endpoint.getName(), response.getResult()));
			}
		} catch (IOException e) {
			LOG.error(e.getMessage());
		} finally {
			attemps = 0;
		}
	}
}
