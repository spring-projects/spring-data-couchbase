/*
 * Copyright 2014 the original author or authors.
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

package org.springframework.data.couchbase.repository.cdi;

import static org.junit.Assert.*;

import org.apache.webbeans.cditest.CdiTestContainer;
import org.apache.webbeans.cditest.CdiTestContainerLoader;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.protocol.views.DesignDocument;
import com.couchbase.client.protocol.views.ViewDesign;

/**
 * @author Mark Paluch
 */
public class CdiRepositoryTests {

	private static CdiTestContainer cdiContainer;
	private CdiPersonRepository repository;
	private CouchbaseClient couchbaseClient;

	@BeforeClass
	public static void init() throws Exception {
		cdiContainer = CdiTestContainerLoader.getCdiContainer();
		cdiContainer.startApplicationScope();
		cdiContainer.bootContainer();
	}

	@AfterClass
	public static void shutdown() throws Exception {
		cdiContainer.stopContexts();
		cdiContainer.shutdownContainer();
	}

	@Before
	public void setUp() {
		CdiRepositoryClient repositoryClient = cdiContainer.getInstance(CdiRepositoryClient.class);
		repository = repositoryClient.getCdiPersonRepository();

		couchbaseClient = repositoryClient.getCouchbaseClient();
		createAndWaitForDesignDocs(couchbaseClient);

	}

	private void createAndWaitForDesignDocs(CouchbaseClient client) {
		DesignDocument designDoc = new DesignDocument("person");
		String mapFunction = "function (doc, meta) { if(doc._class == \"" + Person.class.getName()
				+ "\") { emit(null, null); } }";
		designDoc.setView(new ViewDesign("all", mapFunction, "_count"));
		client.createDesignDoc(designDoc);
	}

	/**
	 * @see DATACOUCH-109
	 */
	@Test
	public void testCdiRepository() {
		assertNotNull(repository);
		repository.deleteAll();

		Person bean = new Person("key", "username");

		repository.save(bean);

		assertTrue(repository.exists(bean.getId()));

		Person retrieved = repository.findOne(bean.getId());
		assertNotNull(retrieved);
		assertEquals(bean.getName(), retrieved.getName());
		assertEquals(bean.getId(), retrieved.getId());

	}

	/**
	 * @see DATACOUCH-109
	 */
	@Test
	public void testCustomRepository() {

		assertEquals(2, repository.returnTwo());
	}

}
