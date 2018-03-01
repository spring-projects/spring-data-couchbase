/*
 * Copyright 2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.couchbase.monitor;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;

import com.couchbase.client.java.Bucket;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
;
import org.springframework.data.couchbase.ContainerResourceRunner;
import org.springframework.data.couchbase.IntegrationTestApplicationConfig;
import org.springframework.data.couchbase.TestContainerResource;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @author Michael Nitschinger
 */
@Ignore(value = "Cant run get cluster info on test container")
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = IntegrationTestApplicationConfig.class)
public class ClusterInfoTests {

	/**
	 * Contains a reference to the actual CouchbaseClient.
	 */
	@Autowired
	private Bucket client;

	private ClusterInfo ci;

	@Before
	public void setup() throws Exception {
		ci = new ClusterInfo(client);
	}

	@Test
	public void totalDiskAssigned() {
		assertThat(ci.getTotalDiskAssigned(), greaterThan(0L));
	}

	@Test
	public void totalRAMUsed() {
		assertThat(ci.getTotalRAMUsed(), greaterThan(0L));
	}

}