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
package org.javierdallamore.camel.examples.pubnub;

import java.util.Date;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.dataformat.JsonLibrary;
import org.apache.camel.spring.Main;

public class MyRouteBuilder extends RouteBuilder {
	
	public static void main(String... args) throws Exception {
		Main.main(args);
	}

	public void configure() {
		// TODO Replace with your own subscribe key here
		String subscribeKey = "sub-x-xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx";
		// TODO Replace with your own publish key here
		String publishKey = "pub-x-xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx";
		// TODO Replace with your own channel here
		String channel = "my-channel";
		
		String name = "mypubnub";
		
		String format = "pubnub://%s?publishKey=%s&subscribeKey=%s&channel=%s";
		String pubnubUri = String.format(format, name, publishKey, subscribeKey, channel);
	
		final Employee[] employees = new Employee[] {
				new Employee("Javier", "Dall Amore", 29, new Date(2012, 2, 7), 1000),
				new Employee("Mariano", "War", 22, new Date(2011, 6, 21), 1200),
				new Employee("Luis", "Aguilar", 32, new Date(2014, 4, 5), 1400),
		};
		
		from(pubnubUri)
			.to("file://target/output")
			.to("log://mylog?level=INFO");
		
		from("timer://myTimer?period=10000&repeatCount=3")
				.process(new Processor() {
					public void process(Exchange exchange) throws Exception {
						int counter = exchange.getProperty(Exchange.TIMER_COUNTER, Integer.class);
						exchange.getIn().setBody(employees[counter - 1]);
					}
				})
				.marshal()
				.json(JsonLibrary.Jackson)
				.to(pubnubUri);
	}
}
