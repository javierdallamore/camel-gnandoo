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

import org.apache.camel.Consumer;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.impl.DefaultEndpoint;
import org.apache.camel.spi.UriEndpoint;
import org.apache.camel.spi.UriParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.eventfabric.api.client.EventClient;

/**
 * An endpoint for working with <a
 * href="http://eventfabric.com/">EventFabric</a>
 *
 * @version $Revision: 1.1 $
 */
@UriEndpoint(scheme = "eventfabric", syntax = "", title = "Event Fabric Endpoint")
public class EventFabricEndpoint extends DefaultEndpoint {
	private static final Logger LOG = LoggerFactory
			.getLogger(EventFabricEndpoint.class);
    private EventClient eventClient;
	private final String name;
	@UriParam
	private String channel;
	@UriParam
	private String bucket;
	@UriParam
	private String action;
    @UriParam
    private String inputEncoding;
	@UriParam
	private String key;

	public EventFabricEndpoint(String uri, EventFabricComponent component,
			String name, EventClient eventClient) {
		super(uri, component);
		this.name = name;
        this.eventClient = eventClient;
	}

	@Override
	public boolean isSingleton() {
		return true;
	}

    public EventClient getEventClient() {
        return eventClient;
    }

	@Override
	protected void doStart() throws Exception {
		super.doStart();
	}

	@Override
	protected void doStop() throws Exception {
		super.doStop();
	}

	@Override
	public Consumer createConsumer(Processor processor) throws Exception {
		return new EventFabricConsumer(this, processor);
	}

	@Override
	public Producer createProducer() throws Exception {
		return new EventFabricProducer(this);
	}

	public String getName() {
		return this.name;
	}

	public String getChannel() {
		return channel;
	}

	public void setChannel(String channel) {
		this.channel = channel;
	}

	public String getBucket() {
		return bucket;
	}

	public void setBucket(String bucket) {
		this.bucket = bucket;
	}

	public String getAction() {
		return action;
	}

	public void setAction(String action) {
        this.action = action;
    }

    public String getInputEncoding() {
        return inputEncoding;
    }

    public void setInputEncoding(String encoding) {
        this.inputEncoding = encoding;
    }

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}
}
