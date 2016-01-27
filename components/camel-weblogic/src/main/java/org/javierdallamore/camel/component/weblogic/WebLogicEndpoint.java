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

import java.util.Timer;

import org.apache.camel.Consumer;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.RuntimeCamelException;
import org.apache.camel.api.management.ManagedAttribute;
import org.apache.camel.impl.DefaultEndpoint;
import org.apache.camel.spi.UriEndpoint;
import org.apache.camel.spi.UriParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * An endpoint for working with <a
 * href="http://eventfabric.com/">EventFabric</a>
 *
 * @version $Revision: 1.1 $
 */
@UriEndpoint(scheme = "weblogic", syntax = "", title = "WebLogic Endpoint")
public class WebLogicEndpoint extends DefaultEndpoint {
	private static final Logger LOG = LoggerFactory
			.getLogger(WebLogicEndpoint.class);
    @UriParam
    private String name;
	@UriParam
	private String host;
	@UriParam
	private String user;
	@UriParam
	private String password; 
	@UriParam
	private String cf;
	@UriParam
	private String queue;
	@UriParam
	private String destinationName;
    @UriParam
    private Timer timer;
    @UriParam
    private long period = 1000;
    @UriParam
    private long delay = 1000;
    @UriParam
    private boolean readMetrics = true;
    @UriParam
    private boolean readMessage = false;
    @UriParam
    private boolean readMessageBody = false;

	public WebLogicEndpoint(String uri, WebLogicComponent component,
			String name) {
		super(uri, component);
		this.name = name;
	}

	@ManagedAttribute(description = "Singleton")
	public boolean isSingleton() {
		return true;
	}

	@Override
    protected void doStart() throws Exception {
        super.doStart();
        // do nothing, the timer will be set when the first consumer will request it
    }

	@Override
    protected void doStop() throws Exception {
        setTimer(null);
        super.doStop();
    }

	@Override
	public Consumer createConsumer(Processor processor) throws Exception {
		return new WebLogicConsumer(this, processor);
	}

	@Override
	public Producer createProducer() throws Exception {
		LOG.error("Cannot produce to a WebLogicEndpoint: " + getEndpointUri());
		throw new RuntimeCamelException("Cannot produce to a WebLogicEndpoint: " + getEndpointUri());
	}

    @ManagedAttribute
    public boolean isMultipleConsumersSupported() {
        return true;
    }
	
	public Timer getTimer(WebLogicConsumer consumer) {
        if (timer != null) {
            // use custom timer
            return timer;
        }
        return getComponent().getTimer(consumer);
    }

    public void setTimer(Timer timer) {
        this.timer = timer;
    }

    public void removeTimer(WebLogicConsumer consumer) {
        if (timer == null) {
            // only remove timer if we are not using a custom timer
            getComponent().removeTimer(consumer);
        }
    }
    
    @ManagedAttribute(description = "Name")
    public String getName() {
        if (name == null) {
        	name = getEndpointUri();
        }
        return name;
    }

    @ManagedAttribute(description = "Name")
    public void setName(String name) {
        this.name = name;
    }

    @Override
    public WebLogicComponent getComponent() {
        return (WebLogicComponent) super.getComponent();
    }
    
	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getCf() {
		return cf;
	}

	public void setCf(String cf) {
		this.cf = cf;
	}

	public String getQueue() {
		return queue;
	}

	public void setQueue(String queue) {
		this.queue = queue;
	}

	public String getDestinationName() {
		return destinationName;
	}

	public void setDestinationName(String destinationName) {
		this.destinationName = destinationName;
	}

	
	public long getPeriod() {
		return period;
	}

	public void setPeriod(long period) {
		this.period = period;
	}

	public long getDelay() {
		return delay;
	}

	public void setDelay(long delay) {
		this.delay = delay;
	}

	public boolean isReadMessage() {
		return readMessage;
	}

	public void setReadMessage(boolean readMessage) {
		this.readMessage = readMessage;
	}

	public boolean isReadMessageBody() {
		return readMessageBody;
	}

	public void setReadMessageBody(boolean readMessageBody) {
		this.readMessageBody = readMessageBody;
	}
	
	public boolean isReadMetrics() {
		return readMetrics;
	}

	public void setReadMetrics(boolean readMetrics) {
		this.readMetrics = readMetrics;
	}
}
