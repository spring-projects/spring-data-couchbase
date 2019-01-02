/*
 * Copyright 2017-2019 the original author or authors.
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

package org.springframework.data.couchbase.repository;

import static org.junit.Assert.*;

import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.data.couchbase.ContainerResourceRunner;
import org.springframework.data.couchbase.ReactiveIntegrationTestApplicationConfig;
import org.springframework.data.couchbase.repository.config.ReactiveRepositoryOperationsMapping;
import org.springframework.data.couchbase.repository.support.IndexManager;
import org.springframework.data.couchbase.repository.support.ReactiveCouchbaseRepositoryFactory;
import org.springframework.data.domain.Sort;
import org.springframework.data.repository.core.support.ReactiveRepositoryFactorySupport;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;

/**
 * This tests ReactiveSortingRepository features in the Couchbase connector.
 *
 * @author Subhashni Balakrishnan
 */
@RunWith(ContainerResourceRunner.class)
@ContextConfiguration(classes = ReactiveIntegrationTestApplicationConfig.class)
@TestExecutionListeners(PartyPopulatorListener.class)
public class ReactiveN1qlCouchbaseRepositoryTests {

	@Autowired
	private ReactiveRepositoryOperationsMapping operationsMapping;

	@Autowired
	private IndexManager indexManager;

	private ReactivePartySortingRepository repository;

	private ReactivePartyRepository partyRepository;

	private ItemRepository itemRepository;

	private final String KEY_PARTY = "ReactiveParty1";
	private final String KEY_ITEM = "ReactiveItem1";


	@Before
	public void setup() throws Exception {
		ReactiveRepositoryFactorySupport factory = new ReactiveCouchbaseRepositoryFactory(operationsMapping, indexManager);
		repository = factory.getRepository(ReactivePartySortingRepository.class);
		partyRepository = factory.getRepository(ReactivePartyRepository.class);
		itemRepository = factory.getRepository(ItemRepository.class);
	}

	@After
	public void cleanUp() {
		try { itemRepository.deleteById(KEY_ITEM); } catch (DataRetrievalFailureException e) {}
		try { partyRepository.deleteById(KEY_PARTY); } catch (DataRetrievalFailureException e) {}
	}

	@Test
	public void shouldFindAllWithSort() {
		Iterable<Party> allByAttendanceDesc = repository.findAll(new Sort(Sort.Direction.DESC, "attendees")).collectList().block();
		long previousAttendance = Long.MAX_VALUE;
		for (Party party : allByAttendanceDesc) {
			assertTrue(party.getAttendees() <= previousAttendance);
			previousAttendance = party.getAttendees();
		}
		assertFalse("Expected to find several parties", previousAttendance == Long.MAX_VALUE);
	}

	@Test
	public void shouldSortOnRenamedFieldIfJsonNameIsProvidedInSort() {
		Iterable<Party> parties = repository.findAll(new Sort(Sort.Direction.DESC, "desc")).collectList().block();
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
		Iterable<Party> parties = repository.findAll(new Sort(new Sort.Order(Sort.Direction.DESC, "desc").ignoreCase())).collectList().block();
		String previousDesc = null;
		for(Party party : parties) {
			if (previousDesc != null) {
				assertTrue(party.getDescription().compareToIgnoreCase(previousDesc) <= 0);
			}
			previousDesc = party.getDescription();
		}
		assertNotNull("Expected to find several parties", previousDesc);
	}

    @Test
    public void testCustomSpelCountQuery() {
        long count = partyRepository.countCustom().block();
        assertTrue("Count query for parties should be atleast 12", count >= 12);
    }

    @Test
    public void testPartTreeQuery() {
        long count = partyRepository.countAllByDescriptionNotNull().block();
        assertTrue("Count query for parties with description not null should be atleast 12", count >= 12);
    }

	@Test
	public void testSpelDateConvertion() {
		final String key = "testReactiveSpelDateConvertion";
		Calendar cal = Calendar.getInstance();
		cal.clear();
		cal.set(2018, Calendar.SEPTEMBER, 15);
		Date date = cal.getTime();
		partyRepository.save(new Party(key, "", "", date, 0, null)).block();
		List<Party> partyList = partyRepository.getByEventDate(date).collectList().block();
		assertTrue(partyList.size() == 1);
		assertEquals("Key mismatch", partyList.get(0).getKey(), key);
	}

    @Test
    public void testN1qlQueryWithInvalidValue() {
        partyRepository.save(new Party("testReactiveN1qlQueryWithInvalidValue", "", "testReactiveN1qlQueryWithInvalidValue", null, 0, null));
        final String description = "testReactiveN1qlQueryWithInvalidValue* OR `description` LIKE \"\"";
        List<Party> partyList = partyRepository.findByDescriptionStartingWith(description).collectList().block();
        assertTrue(partyList.size() == 0);
    }
}
