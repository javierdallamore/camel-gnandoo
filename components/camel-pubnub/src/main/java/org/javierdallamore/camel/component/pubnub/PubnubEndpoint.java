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
package org.javierdallamore.camel.component.pubnub;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.camel.Consumer;
import org.apache.camel.Exchange;
import org.apache.camel.ExchangePattern;
import org.apache.camel.Message;
import org.apache.camel.PollingConsumer;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.impl.DefaultEndpoint;
import org.apache.camel.spi.UriEndpoint;
import org.apache.camel.spi.UriParam;
import org.apache.camel.util.ObjectHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pubnub.api.Callback;
import com.pubnub.api.Pubnub;
import com.pubnub.api.PubnubError;

/**
 * An endpoint for working with <a href="http://pubnub.com/">Pubnub</a>
 *
 * @version $Revision: 1.1 $
 */
@UriEndpoint(scheme = "pubnub")
public class PubnubEndpoint extends DefaultEndpoint {
	private static final Logger LOG = LoggerFactory.getLogger(PubnubEndpoint.class);
	
	private final PubnubComponent component;
	private final String name;
	private final AtomicInteger consumers = new AtomicInteger(0);
	@UriParam
	private String publishKey;
	@UriParam
	private String subscribeKey;
	@UriParam
	private String channel;
	private Pubnub pubnub;

	public PubnubEndpoint(String uri, PubnubComponent component, String name) {
		super(uri, component);
		this.component = component;
		this.name = name;
	}

	@Override
	public boolean isSingleton() {
		return true;
	}

	@Override
	protected void doStart() throws Exception {
		createPubnub();
		super.doStart();
	}

	@Override
	protected void doStop() throws Exception {
		pubnub.unsubscribeAll();
		pubnub = null;
		super.doStop();
	}

	@Override
	public Consumer createConsumer(Processor processor) throws Exception {
		// TODO Auto-generated method stub
		consumers.incrementAndGet();
		return new PubnubConsumer(this, processor);
	}

	@Override
	public Producer createProducer() throws Exception {
		// TODO Auto-generated method stub
		return new PubnubProducer(this);
	}

	public void createPubnub() {
		LOG.info(String.format("Starting pubnub with %s and %s", publishKey, subscribeKey));
		this.pubnub = new Pubnub(publishKey, subscribeKey);
	}
	
	public String getChannel() {
		return channel;
	}

	public void setChannel(String channel) {
		this.channel = channel;
	}

	public void setPubnub(Pubnub pubnub) {
		this.pubnub = pubnub;
	}
	
	public Pubnub getPubnub() {
		if (this.pubnub != null) {
			createPubnub();
		}
		return this.pubnub;
	}

	public String getName() {
		return name;
	}
	
	public String getPublishKey() {
		return publishKey;
	}

	public void setPublishKey(String publishKey) {
		this.publishKey = publishKey;
	}

	public String getSubscribeKey() {
		return subscribeKey;
	}

	public void setSubscribeKey(String subscribeKey) {
		this.subscribeKey = subscribeKey;
	}

}
