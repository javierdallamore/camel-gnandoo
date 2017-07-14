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

import java.util.Map;

import org.apache.camel.Endpoint;
import org.apache.camel.impl.DefaultComponent;
import org.apache.camel.CamelContext;
import org.apache.camel.util.CamelContextHelper;
import com.eventfabric.api.client.EventClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A component for working with <a href="http://eventfabric.com/">EventFabric</a>
 *
 * @version $Revision: 1.1 $
 */
public class EventFabricComponent extends DefaultComponent {
	private static final Logger LOG = LoggerFactory
			.getLogger(EventFabricComponent.class);
	private EventClient eventClient;

	/**
	 * Creates an EventFabric endpoint. {@inheritDoc}
	 *
	 * @param uri
	 * @param remaining
	 * @param parameters
	 * @return Endpoint
	 * @throws java.lang.Exception
	 */
	@Override
	protected Endpoint createEndpoint(String uri, String remaining,
			Map<String, Object> parameters) throws Exception {
        LOG.info("Loading" + remaining);
        eventClient = CamelContextHelper.mandatoryLookup(getCamelContext(), remaining, EventClient.class);

		LOG.info(eventClient.toString());

		Endpoint endpoint = new EventFabricEndpoint(uri, this, remaining, eventClient);
		setProperties(endpoint, parameters);
		return endpoint;
	}
}
