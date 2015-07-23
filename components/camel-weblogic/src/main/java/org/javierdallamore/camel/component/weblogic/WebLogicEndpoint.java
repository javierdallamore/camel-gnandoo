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
import java.util.Date;
import java.util.Hashtable;
import java.util.Timer;

import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.apache.camel.Consumer;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.RuntimeCamelException;
import org.apache.camel.api.management.ManagedAttribute;
import org.apache.camel.component.timer.TimerComponent;
import org.apache.camel.component.timer.TimerConsumer;
import org.apache.camel.impl.DefaultEndpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.camel.spi.UriEndpoint;
import org.apache.camel.spi.UriParam;
/**
 * An endpoint for working with <a
 * href="http://eventfabric.com/">EventFabric</a>
 *
 * @version $Revision: 1.1 $
 */
@UriEndpoint(scheme = "weblogic")
public class WebLogicEndpoint extends DefaultEndpoint {
	private static final Logger LOG = LoggerFactory
			.getLogger(WebLogicEndpoint.class);
    @UriParam
    private String name;
	@UriParam
	private String url;
	@UriParam
	private String user;
	@UriParam
	private String password; 
	@UriParam
	private String cf;
	@UriParam
	private String queue;
    @UriParam
    private Timer timer;
    @UriParam
    private long period = 1000;
    @UriParam
    private long delay = 1000;

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
    
	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
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
}
