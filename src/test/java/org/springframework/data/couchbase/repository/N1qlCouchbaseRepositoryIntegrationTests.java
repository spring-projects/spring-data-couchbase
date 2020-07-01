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

package org.springframework.data.couchbase.repository;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataRetrievalFailureException;
//import org.springframework.data.couchbase.ContainerResourceRunner;
import org.springframework.data.couchbase.IntegrationTestApplicationConfig;
import org.springframework.data.couchbase.repository.config.EnableCouchbaseRepositories;
// must not be in same package as CouchbaseRepository, otherwise AutoWired will fail on couchbaseRepository
import org.springframework.data.couchbase.repo.ItemRepository;
import org.springframework.data.couchbase.repo.PartyRepository;
import org.springframework.data.couchbase.repo.PartyPagingRepository;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.mapping.model.MappingInstantiationException;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * This has been copied from 3.2.x for testing backport of fix in DATACOUCH-484
 * This and dependent class have been copied from src/integration to src/test
 * and the repository classes have been moved from org.springframework.data.couchbase.repository
 * to org.springframework.data.couchbase.repo so that
 * org.springframework.data.couchbase.repository.CouchbaseRepository is not instantiated by
 * @Autowired
 *
 * This tests PaginAndSortingRepository features in the Couchbase connector.
 *
 * @author Simon Baslé
 * @author Subhashni Balakrishnan
 * @author Michael Reiche
 */
// @RunWith(ContainerResourceRunner.class)
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = IntegrationTestApplicationConfig.class)
@SpringJUnitConfig(IntegrationTestApplicationConfig.class)
@TestExecutionListeners(PartyPopulatorListener.class)
@EnableCouchbaseRepositories
public class N1qlCouchbaseRepositoryIntegrationTests {

	@Autowired private PartyPagingRepository repository;

	@Autowired private PartyRepository partyRepository;

	@Autowired private ItemRepository itemRepository;

	private final String KEY_PARTY = "Party1";
	private final String KEY_ITEM = "Item1";

	@Before
	public void setup() throws Exception {
		partyRepository.save(new Party(KEY_PARTY, "partyName", "MatchingDescription", null, 1, null));
		itemRepository.save(new Item(KEY_ITEM, "MatchingDescription"));
	}

	@After
	public void cleanUp() {
		try {
			itemRepository.deleteById(KEY_ITEM);
		} catch (DataRetrievalFailureException e) {}
		try {
			partyRepository.deleteById(KEY_PARTY);
		} catch (DataRetrievalFailureException e) {}
	}

	@Test
	public void shouldBeThreadsafe() {
		// This doesn't guarantee it, but we should catch most thread issues without
		// taking too long here...
		int runs = 50;
		for (int i = 0; i < runs; i++) {
			doShouldBeThreadsafe();
		}
	}

	public void doShouldBeThreadsafe() {
		int threads = 50;
		for (int thread = 0; thread < threads; thread++) {
			partyRepository.save(new Party(KEY_PARTY + thread, "Party like it's 199" + thread, "", null, 1, null));
		}
		ExecutorService service = Executors.newFixedThreadPool(threads);
		List<Callable<Boolean>> callables = new ArrayList<>();
		for (int thread = 0; thread < threads; ++thread) {
			final int counter = thread;
			Callable<Boolean> booleanSupplier = () -> {
				String expectedName = "Party like it's 199" + counter;
				String foundName = partyRepository.findByName(expectedName).get(0).getName();
				return expectedName.equals(foundName); // should never get false
			};
			callables.add(booleanSupplier);
		}
		try {
			List<Future<Boolean>> futures = service.invokeAll(callables);
			service.shutdown();
			service.awaitTermination(5, TimeUnit.SECONDS);
			for (Future<Boolean> future : futures) {
				assertTrue(future.get());
			}
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
			fail("Threads failed to run " + e.getMessage());
		}
		for (int thread = 0; thread < threads; thread++) {
			partyRepository.delete(new Party(KEY_PARTY + thread, "Party like it's 199" + thread, "", null, 1, null));
		}
	}

	@Test
	public void shouldFindAllWithSort() {
		Iterable<Party> allByAttendanceDesc = repository.findAll(Sort.by(Sort.Direction.DESC, "attendees"));
		long previousAttendance = Long.MAX_VALUE;
		for (Party party : allByAttendanceDesc) {
			assertTrue(party.getAttendees() <= previousAttendance);
			previousAttendance = party.getAttendees();
		}
		assertFalse("Expected to find several parties", previousAttendance == Long.MAX_VALUE);
	}

	@Test
	public void shouldSortOnRenamedFieldIfJsonNameIsProvidedInSort() {
		Iterable<Party> parties = repository.findAll(Sort.by(Sort.Direction.DESC, "desc"));
		String previousDesc = null;
		for (Party party : parties) {
			if (previousDesc != null) {
				assertTrue(party.getDescription().compareTo(previousDesc) <= 0);
			}
			previousDesc = party.getDescription();
		}
		assertNotNull("Expected to find several parties", previousDesc);
	}

	@Test
	public void shouldSortWithoutCaseSensitivity() {
		Iterable<Party> parties = repository.findAll(Sort.by(new Sort.Order(Sort.Direction.DESC, "desc").ignoreCase()));
		String previousDesc = null;
		for (Party party : parties) {
			if (previousDesc != null) {
				assertTrue(party.getDescription().compareToIgnoreCase(previousDesc) <= 0);
			}
			previousDesc = party.getDescription();
		}
		assertNotNull("Expected to find several parties", previousDesc);
	}

	@Test
	public void shouldPageThroughEntities() {
		Pageable pageable = PageRequest.of(0, 8);

		Page<Party> page1 = repository.findAll(pageable);
		assertTrue("Query for parties should be atleast 12", page1.getTotalElements() >= 12);
		assertEquals(8, page1.getNumberOfElements());
	}

	@Test
	public void shouldPageThroughSortedEntities() {
		Pageable pageable = PageRequest.of(0, 8, Sort.Direction.DESC, "attendees");

		Page<Party> page1 = repository.findAll(pageable);
		assertTrue("Query for parties should be atleast 12", page1.getTotalElements() >= 12);
		assertEquals(8, page1.getNumberOfElements());

		List<Party> parties = page1.getContent();
		Long previousAttendees = null;
		for (Party party : parties) {
			if (previousAttendees != null) {
				assertTrue(party.getAttendees() <= previousAttendees);
			}
			previousAttendees = party.getAttendees();
		}
	}

	@Test
	public void testWrapWhereCriteria() {
		List<Party> partyList = partyRepository.findByDescriptionOrName("MatchingDescription", "partyName");
		assertTrue(partyList.size() == 1);
	}

	@Test
	public void shouldPageWithStringBasedQuery() {
		Pageable pageable = PageRequest.of(0, 8, Sort.Direction.DESC, "attendees");
		Page<Party> page1 = partyRepository.findPartiesWithAttendee(1, pageable);
		assertTrue("Query for parties with attendees should be atleast 12", page1.getTotalElements() >= 12);
		assertEquals(8, page1.getNumberOfElements());

		List<Party> parties = page1.getContent();
		Long previousAttendees = null;
		for (Party party : parties) {
			if (previousAttendees != null) {
				assertTrue(party.getAttendees() <= previousAttendees);
			}
			previousAttendees = party.getAttendees();
		}
		Page<Party> page2 = partyRepository.findPartiesWithAttendee(1, page1.nextPageable());
		assertEquals(8, page2.getNumberOfElements());
		parties = page2.getContent();
		for (Party party : parties) {
			if (previousAttendees != null) {
				assertTrue(party.getAttendees() <= previousAttendees);
			}
			previousAttendees = party.getAttendees();
		}
	}

	// Fails on deserialization as a different entity item is also present
	@Test(expected = MappingInstantiationException.class)
	public void shouldFailWithMissingFilterStringBasedQuery() {
		Sort sort = Sort.by(Sort.Direction.DESC, "attendees");
		partyRepository.findParties(sort);
	}

	@Test
	public void testDeleteQuery() {
		partyRepository.save(new Party("testDeleteQuery", "delete", "delete", null, 0, null));
		List<Party> partyList = partyRepository.removeByDescriptionOrName("delete", "delete");
		assertTrue(partyList.size() == 1);
	}

	@Test
	public void testSpelDateConvertion() {
		final String key = "testSpelDateConvertion";
		Calendar cal = Calendar.getInstance();
		cal.clear();
		cal.set(2018, Calendar.SEPTEMBER, 10);
		Date date = cal.getTime();
		partyRepository.save(new Party(key, "", "", date, 0, null));
		List<Party> partyList = partyRepository.getByEventDate(date);
		assertTrue(partyList.size() == 1);
		assertEquals("Key mismatch", partyList.get(0).getKey(), key);
	}

	@Test
	public void testN1qlQueryWithInvalidValue() {
		partyRepository
				.save(new Party("testN1qlQueryWithInvalidValue", "", "testN1qlQueryWithInvalidValue", null, 0, null));
		final String description = "testN1qlQueryWithInvalidValue* OR `description` LIKE \"\"";
		List<Party> partyList = partyRepository.findByDescriptionStartingWith(description);
		assertTrue(partyList.size() == 0);
	}

}
