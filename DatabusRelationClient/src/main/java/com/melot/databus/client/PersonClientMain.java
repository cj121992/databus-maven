package com.melot.databus.client;

/*
 *
 * Copyright 2013 LinkedIn Corp. All rights reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

import com.linkedin.databus.client.DatabusHttpClientImpl;
import com.melot.sdk.core.util.MelotBeanFactory;

public class PersonClientMain {
	static final String SOURCES[] = {
			"com.linkedin.events.example.appstable.UserRelation",
			"com.linkedin.events.example.appstable.ActorRelation",
			"com.linkedin.events.example.appstable.ActorInfo",
			"com.linkedin.events.example.appstable.RoomInfo",
			"com.linkedin.events.example.appstable.ActorResource" };
	static final String SOURCES_STR = "com.linkedin.events.example.appstable.UserRelation"
			+ ",com.linkedin.events.example.appstable.ActorRelation"
			+ ",com.linkedin.events.example.appstable.ActorInfo"
			+ ",com.linkedin.events.example.appstable.RoomInfo"
			+ ",com.linkedin.events.example.appstable.ActorResource";
	static final String USER_SOURCE = "com.linkedin.events.example.appstable.UserRelation";

	static final String ACTOR_SOURCE = "com.linkedin.events.example.appstable.ActorRelation";

	static final String[] RELATION_SOURCES = { USER_SOURCE, ACTOR_SOURCE };

	public static void main(String[] args) throws Exception {

		MelotBeanFactory.init("classpath*:conf/spring-bean-container*.xml");
		DatabusHttpClientImpl.Config configBuilder = new DatabusHttpClientImpl.Config();

		// Try to connect to a relay on localhost
		// if not has key = "1" relay, will put
//		 configBuilder.getRuntime().getRelay("1").setHost("localhost");
		configBuilder.getRuntime().getRelay("1").setHost("10.0.3.157");
		configBuilder.getRuntime().getRelay("1").setPort(11115);
		configBuilder.getRuntime().getRelay("1").setSources(SOURCES_STR);
		// configBuilder.setSkipWTE(true);

//		configBuilder.getRuntime().getBootstrap().setEnabled(true);
//		configBuilder.getRuntime().getBootstrap()
//				.setServicesList("10.0.3.157:11111:" + SOURCES_STR);

		// configBuilder.getRuntime().getRelay("2").setHost("10.0.3.157");
		// configBuilder.getRuntime().getRelay("2").setPort(11115);
		// configBuilder.getRuntime().getRelay("2").setSources(USER_SOURCE);
		// Instantiate a client using command-line parameters if any
		DatabusHttpClientImpl client = DatabusHttpClientImpl.createFromCli(
				args, configBuilder);
		// register callbacks
		// PersonConsumerPg personConsumer = new PersonConsumerPg();
		RelationConsumer relationConsumer = (RelationConsumer) MelotBeanFactory
				.getBean("relationConsumer");
		ActorRelationConsumer actorRelationConsumer = (ActorRelationConsumer) MelotBeanFactory
				.getBean("actorRelationConsumer");
		// PersonConsumer personConsumer = new PersonConsumer();
		client.registerDatabusStreamListener(actorRelationConsumer, null,
				ACTOR_SOURCE);
		client.registerDatabusStreamListener(relationConsumer, null,
				USER_SOURCE);
//		client.registerDatabusBootstrapListener(actorRelationConsumer, null,
//				ACTOR_SOURCE);
//		client.registerDatabusBootstrapListener(relationConsumer, null,
//				USER_SOURCE);

		// fire off the Databus client
		client.startAndBlock();

	}

}
