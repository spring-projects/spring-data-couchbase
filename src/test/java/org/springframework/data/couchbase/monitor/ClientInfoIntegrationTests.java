/*
 * Copyright 2013-2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.couchbase.monitor;


import com.couchbase.client.java.Bucket;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.couchbase.ContainerResourceRunner;
import org.springframework.data.couchbase.IntegrationTestApplicationConfig;
import org.springframework.test.context.ContextConfiguration;

import static org.assertj.core.api.Assertions.*;

/**
 * @author Michael Nitschinger
 */
@RunWith(ContainerResourceRunner.class)
@ContextConfiguration(classes = IntegrationTestApplicationConfig.class)
public class ClientInfoIntegrationTests {

	/**
	 * Contains a reference to the actual CouchbaseClient.
	 */
	@Autowired
	private Bucket client;

	private ClientInfo ci;

	@Before
	public void setup() throws Exception {
		ci = new ClientInfo(client);
	}

	@Test
	public void hostNames() {
		String hostnames = ci.getHostNames();
		assertThat(hostnames).isNotEmpty();
	}

}
