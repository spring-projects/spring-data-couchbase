/*
 * Copyright 2014-2019 the original author or authors.
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

package org.springframework.data.couchbase.repository.cdi;

import static org.junit.Assert.*;

import java.util.Optional;

import com.couchbase.client.java.Bucket;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.data.couchbase.ContainerResourceRunner;
import org.springframework.test.context.ContextConfiguration;

import javax.enterprise.inject.se.SeContainer;
import javax.enterprise.inject.se.SeContainerInitializer;

/**
 * @author Mark Paluch
 */
@SuppressWarnings("SpringJavaAutowiringInspection")
@RunWith(ContainerResourceRunner.class)
@ContextConfiguration(classes = CdiRepositoryIntegrationTests.class)
public class CdiRepositoryIntegrationTests {

	private static SeContainer cdiContainer;
	private CdiPersonRepository repository;
	private QualifiedPersonRepository qualifiedPersonRepository;
	private Bucket couchbaseClient;

	@BeforeClass
	public static void init() {
		cdiContainer = SeContainerInitializer.newInstance() //
						.disableDiscovery() //
						.addPackages(CdiRepositoryClient.class) //
						.initialize();
	}

	@AfterClass
	public static void shutdown() {
		cdiContainer.close();
	}

	@Before
	public void setUp() {

		CdiRepositoryClient repositoryClient = cdiContainer.select(CdiRepositoryClient.class).get();
		repository = repositoryClient.getCdiPersonRepository();
		qualifiedPersonRepository = repositoryClient.getQualifiedPersonRepository();

		couchbaseClient = repositoryClient.getCouchbaseClient();
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

		assertTrue(repository.existsById(bean.getId()));

		Optional<Person> retrieved = repository.findById(bean.getId());
		assertTrue(retrieved.isPresent());
		retrieved.ifPresent(actual -> {
			assertEquals(bean.getName(), actual.getName());
			assertEquals(bean.getId(), actual.getId());
		});
	}

	/**
	 * @see DATACOUCH-203
	 */
	@Test
	public void testQualifiedCdiRepository() {
		assertNotNull(qualifiedPersonRepository);
		qualifiedPersonRepository.deleteAll();

		Person bean = new Person("key", "username");

		qualifiedPersonRepository.save(bean);

		assertTrue(qualifiedPersonRepository.existsById(bean.getId()));

		Optional<Person> retrieved = qualifiedPersonRepository.findById(bean.getId());
		assertTrue(retrieved.isPresent());
		retrieved.ifPresent(actual -> {
			assertEquals(bean.getName(), actual.getName());
			assertEquals(bean.getId(), actual.getId());
		});
	}

	/**
	 * @see DATACOUCH-109
	 */
	@Test
	public void testCustomRepository() {

		assertEquals(2, repository.returnTwo());
	}

}
