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

import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.camel.Endpoint;
import org.apache.camel.component.timer.TimerConsumer;
import org.apache.camel.component.timer.TimerEndpoint;
import org.apache.camel.impl.DefaultComponent;
import org.apache.camel.impl.UriEndpointComponent;

/**
 * A component for working with <a href="http://weblogic.com/">WebLogic</a>
 *
 * @version $Revision: 1.1 $
 */
public class WebLogicComponent extends UriEndpointComponent {
	private final Map<String, Timer> timers = new HashMap<String, Timer>();
    private final Map<String, AtomicInteger> refCounts = new HashMap<String, AtomicInteger>();

    public WebLogicComponent() {
        super(WebLogicEndpoint.class);
    }

    public Timer getTimer(WebLogicConsumer consumer) {
        String key = consumer.getEndpoint().getName();
        Timer answer;
        synchronized (timers) {
            answer = timers.get(key);
            if (answer == null) {
                // the timer name is also the thread name, so lets resolve a name to be used
                String name = consumer
                		.getEndpoint()
                		.getCamelContext()
                		.getExecutorServiceManager()
                		.resolveThreadName("weblogic://" + consumer.getEndpoint().getName());
                answer = new Timer(name, false);
                timers.put(key, answer);
                // store new reference counter
                refCounts.put(key, new AtomicInteger(1));
            } else {
                // increase reference counter
                AtomicInteger counter = refCounts.get(key);
                if (counter != null) {
                    counter.incrementAndGet();
                }
            }
        }
        return answer;
    }

    public void removeTimer(WebLogicConsumer consumer) {
        String key = consumer.getEndpoint().getName();
        synchronized (timers) {
            // decrease reference counter
            AtomicInteger counter = refCounts.get(key);
            if (counter != null && counter.decrementAndGet() <= 0) {
                refCounts.remove(key);
                // remove timer as its no longer in use
                Timer timer = timers.remove(key);
                if (timer != null) {
                    timer.cancel();
                }
            }
        }
    }

    @Override
    protected Endpoint createEndpoint(String uri, String remaining, Map<String, Object> parameters) throws Exception {
        WebLogicEndpoint answer = new WebLogicEndpoint(uri, this, remaining);
        setProperties(answer, parameters);
        return answer;
    }
	
	@Override
    protected void doStop() throws Exception {
        Collection<Timer> collection = timers.values();
        for (Timer timer : collection) {
            timer.cancel();
        }
        timers.clear();
        refCounts.clear();
    }
}
