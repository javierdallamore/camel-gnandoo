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

import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultProducer;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pubnub.api.Callback;
import com.pubnub.api.Pubnub;
import com.pubnub.api.PubnubError;
/**
 * @version $Revision: 1.1 $
 */
public class PubnubProducer extends DefaultProducer {
	private static final Logger LOG = LoggerFactory.getLogger(PubnubProducer.class);
	private final PubnubEndpoint endpoint;

	public PubnubProducer(PubnubEndpoint endpoint) {
		super(endpoint);
		this.endpoint = endpoint;
	}

	/**
	 *
	 * @param exchange
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void process(Exchange exchange) throws Exception {
		Pubnub pubnub = this.endpoint.getPubnub();
		String channel = this.endpoint.getChannel();
		Callback callback = new Callback() {
			public void successCallback(String channel, Object response) {
				String msg = String.format("Pubnub response for channel %s is %s", channel, response.toString());
				LOG.info(msg);
			}

			public void errorCallback(String channel, PubnubError error) {
				String msg = String.format("Pubnub response for channel %s is %s", channel, error.toString());
				LOG.error(msg);
			}
		};
		JSONObject msg = new JSONObject(exchange.getIn().getBody(String.class));
		
		pubnub.publish(channel, msg, callback);
	}
}
