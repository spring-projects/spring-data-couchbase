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

package org.springframework.data.couchbase.repository;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.data.couchbase.IntegrationTestApplicationConfig;
import org.springframework.data.couchbase.repository.config.RepositoryOperationsMapping;
import org.springframework.data.couchbase.repository.support.CouchbaseRepositoryFactory;
import org.springframework.data.couchbase.repository.support.IndexManager;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.mapping.model.MappingInstantiationException;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.List;

/**
 * This tests PaginAndSortingRepository features in the Couchbase connector.
 *
 * @author Simon Baslé
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = IntegrationTestApplicationConfig.class)
@TestExecutionListeners(PartyPopulatorListener.class)
public class N1qlCouchbaseRepositoryTests {

  @Autowired
  private RepositoryOperationsMapping operationsMapping;

  @Autowired
  private IndexManager indexManager;

  private PartyPagingRepository repository;

  private PartyRepository partyRepository;

  private ItemRepository itemRepository;

  private final String KEY_PARTY = "Party1";
  private final String KEY_ITEM = "Item1";


  @Before
  public void setup() throws Exception {
    RepositoryFactorySupport factory = new CouchbaseRepositoryFactory(operationsMapping, indexManager);
    repository = factory.getRepository(PartyPagingRepository.class);
    partyRepository = factory.getRepository(PartyRepository.class);
    itemRepository = factory.getRepository(ItemRepository.class);
    partyRepository.save(new Party(KEY_PARTY, "partyName", "MatchingDescription", null, 1, null));
    itemRepository.save(new Item(KEY_ITEM, "MatchingDescription"));
  }

  @After
  public void cleanUp() {
    try { itemRepository.delete(KEY_ITEM); } catch (DataRetrievalFailureException e) {}
    try { partyRepository.delete(KEY_PARTY); } catch (DataRetrievalFailureException e) {}
  }

  @Test
  public void shouldFindAllWithSort() {
    Iterable<Party> allByAttendanceDesc = repository.findAll(new Sort(Sort.Direction.DESC, "attendees"));
    long previousAttendance = Long.MAX_VALUE;
    for (Party party : allByAttendanceDesc) {
      assertTrue(party.getAttendees() <= previousAttendance);
      previousAttendance = party.getAttendees();
    }
    assertFalse("Expected to find several parties", previousAttendance == Long.MAX_VALUE);
  }

  @Test
  public void shouldSortOnRenamedFieldIfJsonNameIsProvidedInSort() {
    Iterable<Party> parties = repository.findAll(new Sort(Sort.Direction.DESC, "desc"));
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
    Iterable<Party> parties = repository.findAll(new Sort(new Sort.Order(Sort.Direction.DESC, "desc").ignoreCase()));
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
    Pageable pageable = new PageRequest(0, 8);

    Page<Party> page1 = repository.findAll(pageable);
    assertEquals(16, page1.getTotalElements()); //12 generated parties + 4 specifically crafted party
    assertEquals(8, page1.getNumberOfElements());
  }

  @Test
  public void shouldPageThroughSortedEntities() {
    Pageable pageable = new PageRequest(0, 8, Sort.Direction.DESC, "attendees");

    Page<Party> page1 = repository.findAll(pageable);
    assertEquals(16, page1.getTotalElements()); //12 generated parties + 4 specifically crafted party
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
    Pageable pageable = new PageRequest(0, 8, Sort.Direction.DESC, "attendees");
    Page<Party> page1 = partyRepository.findPartiesWithAttendee(1, pageable);
    assertEquals(16, page1.getTotalElements()); //12 generated parties + 4 specifically crafted party
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

  //Fails on deserialization as a different entity item is also present
  @Test(expected = MappingInstantiationException.class)
  public void shouldFailWithMissingFilterStringBasedQuery() {
    Sort sort = new Sort(Sort.Direction.DESC, "attendees");
    List<Party> parties = partyRepository.findParties(sort);
  }
}
