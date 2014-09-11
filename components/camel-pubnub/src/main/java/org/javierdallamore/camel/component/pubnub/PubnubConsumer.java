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

import java.util.Date;

import org.apache.camel.Exchange;
import org.apache.camel.ExchangePattern;
import org.apache.camel.Processor;
import org.apache.camel.impl.DefaultConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pubnub.api.Callback;
import com.pubnub.api.Pubnub;
import com.pubnub.api.PubnubException;
import com.pubnub.api.PubnubError;

import org.apache.camel.AsyncCallback;

/**
 * @version $Revision: 1.1 $
 */
public class PubnubConsumer extends DefaultConsumer {
	private static final Logger LOG = LoggerFactory
			.getLogger(PubnubConsumer.class);
	public final String PUBNUB_EVENT_COUNTER = "CamelPubnubEventCounter";
	public final String PUBNUB_EVENT_FIRED_TIME = "CamelPubnubFiredTime";
	public final String PUBNUB_CHANNEL = "CamelPubnubChannel";
	
	private PubnubEndpoint endpoint;

	public PubnubConsumer(PubnubEndpoint endpoint, Processor processor) {
		super(endpoint, processor);
		this.endpoint = endpoint;
	}

	@Override
	protected void doStart() throws Exception {
		Pubnub pubnub = this.endpoint.getPubnub();
		String channel = this.endpoint.getChannel();
		LOG.info("PUBNUB Consumer started for " + channel);
		try {
			pubnub.subscribe(channel, new Callback() {

				@Override
				public void connectCallback(String channel, Object message) {
					LOG.info("SUBSCRIBE : CONNECT on channel:" + channel
							+ " : " + message.getClass() + " : "
							+ message.toString());
				}

				@Override
				public void disconnectCallback(String channel, Object message) {
					LOG.info("SUBSCRIBE : DISCONNECT on channel:" + channel
							+ " : " + message.getClass() + " : "
							+ message.toString());
				}

				public void reconnectCallback(String channel, Object message) {
					LOG.warn("SUBSCRIBE : RECONNECT on channel:" + channel
							+ " : " + message.getClass() + " : "
							+ message.toString());
				}

				@Override
				public void successCallback(String channel, Object message) {
					LOG.info("SUBSCRIBE : " + channel + " : "
							+ message.getClass() + " : " + message.toString());
					sendPubnubExchange(1, channel, message);
				}

				@Override
				public void errorCallback(String channel, PubnubError error) {
					LOG.error("SUBSCRIBE : ERROR on channel " + channel + " : "
							+ error.toString());
				}
			});
		} catch (PubnubException e) {
			LOG.error("PUBNUB Exception" + e.toString());
		}
	}

	@Override
	protected void doStop() throws Exception {
		Pubnub pubnub = this.endpoint.getPubnub();
		String channel = this.endpoint.getChannel();
		pubnub.unsubscribe(channel);
	}

	protected void sendPubnubExchange(long counter, String channel, Object message) {
		final Exchange exchange = endpoint.createExchange(ExchangePattern.InOnly);
		exchange.setProperty(PUBNUB_EVENT_COUNTER, counter);
		Date now = new Date();
		exchange.setProperty(PUBNUB_EVENT_FIRED_TIME, now);
		exchange.setProperty(PUBNUB_CHANNEL, channel);
		// also set now on in header with same key as quartz to be consistent
		exchange.getIn().setHeader("firedTime", now);
		exchange.getIn().setBody(message.toString());
		if (LOG.isTraceEnabled()) {
			LOG.trace("Pubnub {} is firing #{} count", endpoint.getName(),
					counter);
		}
		if (!endpoint.isSynchronous()) {
			getAsyncProcessor().process(exchange, new AsyncCallback() {
				@Override
				public void done(boolean doneSync) {
					// handle any thrown exception
					if (exchange.getException() != null) {
						getExceptionHandler().handleException(
								"Error processing exchange", exchange,
								exchange.getException());
					}
				}
			});
		} else {
			try {
				getProcessor().process(exchange);
			} catch (Exception e) {
				exchange.setException(e);
			}
			// handle any thrown exception
			if (exchange.getException() != null) {
				getExceptionHandler().handleException(
						"Error processing exchange", exchange,
						exchange.getException());
			}
		}
	}
}
