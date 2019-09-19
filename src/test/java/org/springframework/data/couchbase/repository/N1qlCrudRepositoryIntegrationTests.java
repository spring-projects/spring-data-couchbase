/*
 * Copyright 2013-2019 the original author or authors.
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
import static org.springframework.data.couchbase.CouchbaseTestHelper.getRepositoryWithRetry;

import java.util.Date;
import java.util.List;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Collection;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataRetrievalFailureException;
;
import org.springframework.data.couchbase.ContainerResourceRunner;
import org.springframework.data.couchbase.IntegrationTestApplicationConfig;
import org.springframework.data.couchbase.core.CouchbaseQueryExecutionException;
import org.springframework.data.couchbase.repository.config.RepositoryOperationsMapping;
import org.springframework.data.couchbase.repository.support.CouchbaseRepositoryFactory;
import org.springframework.data.couchbase.repository.support.IndexManager;
import org.springframework.data.geo.Point;
import org.springframework.test.context.ContextConfiguration;

/**
 * @author Simon Baslé
 */
@RunWith(ContainerResourceRunner.class)
@ContextConfiguration(classes = IntegrationTestApplicationConfig.class)
public class N1qlCrudRepositoryIntegrationTests {

  @Autowired
  private Collection client;

  @Autowired
  private RepositoryOperationsMapping operationsMapping;

  @Autowired
  private IndexManager indexManager;

  private PartyRepository partyRepository;
  private ItemRepository itemRepository;

  private static final String KEY_ITEM = "itemNotParty";
  private static final String KEY_PARTY = "partyNotItem";
  private static final String KEY_PARTY_KEYWORD = "partyHasKeyword";

  private static final Item item = new Item(KEY_ITEM, "short description");
  private static final Party party = new Party(KEY_PARTY, "partyName", "short description", new Date(), 120, new Point(500, 500));

  @Before
  public void setup() throws Exception {
    CouchbaseRepositoryFactory factory = new CouchbaseRepositoryFactory(operationsMapping, indexManager);

    partyRepository = getRepositoryWithRetry(factory, PartyRepository.class);
    itemRepository = getRepositoryWithRetry(factory, ItemRepository.class);

    itemRepository.save(item);
    partyRepository.save(party);
  }

  @After
  public void cleanUp() {
    try { itemRepository.deleteById(KEY_ITEM); } catch (DataRetrievalFailureException e) {}
    try { partyRepository.deleteById(KEY_PARTY); } catch (DataRetrievalFailureException e) {}
    try { partyRepository.deleteById(KEY_PARTY_KEYWORD); } catch (DataRetrievalFailureException e) {}
  }

  @Test
  public void shouldDistinguishBetweenItemsAndParties() {
    List<Object> items = itemRepository.findAllByDescriptionNotNull();
    List<Object> parties = partyRepository.findAllByDescriptionNotNull();

    assertTrue(items.contains(item));
    assertTrue(parties.contains(party));

    assertFalse(items.contains(party));
    assertFalse(parties.contains(item));
  }

  @Test
  public void shouldSaveObjectWithN1qlKeywordField() {
    Party partyHasKeyword = new Party(KEY_PARTY_KEYWORD, "party", "desc is a N1QL keyword", new Date(), 40, new Point(500, 500));
    partyRepository.save(partyHasKeyword);
    List<Object> parties = partyRepository.findAllByDescriptionNotNull();

    assertTrue(client.exists(KEY_PARTY_KEYWORD).exists());
    assertTrue(parties.contains(partyHasKeyword));
    for (Object o : parties) {
      if (!(o instanceof Party)) {
        fail("expected only Party objects");
      }
    }
  }

  @Test
  public void shouldGenerateCountProjection() {
    Party partyHasKeyword = new Party(KEY_PARTY_KEYWORD, "party", null, new Date(), 40, new Point(500, 500));
    partyRepository.save(partyHasKeyword);
    long countTotal = partyRepository.count();
    long countCustom = partyRepository.countAllByDescriptionNotNull();
    assertEquals(countTotal - 1, countCustom);
  }

  @Test
  public void shouldCountWhenReturningLongAndUsingStringSelectFromSpEL() {
    Party partyHasKeyword = new Party(KEY_PARTY_KEYWORD, "party", "second party", new Date(), 40, new Point(500, 500));
    partyRepository.save(partyHasKeyword);

    long countTotal = partyRepository.count();
    long countCustom = partyRepository.countCustom();
    assertEquals(countTotal, countCustom);
  }

  @Test
  public void shouldCustomCountWhenReturningLongAndUsingStringWithoutSpEL() {
    Party partyHasKeyword = new Party(KEY_PARTY_KEYWORD, "party", "second party", new Date(), 40, new Point(500, 500));
    partyRepository.save(partyHasKeyword);

    long countTotal = partyRepository.count();
    long countCustom = partyRepository.countCustomPlusFive();
    assertEquals(countTotal + 5, countCustom);
  }

  @Test(expected = CouchbaseQueryExecutionException.class)
  public void shouldFailConversionWithStringReturnType() {
    Party partyHasKeyword = new Party(KEY_PARTY_KEYWORD, "party", "desc is a N1QL keyword", new Date(), 40, new Point(500, 500));
    partyRepository.save(partyHasKeyword);

    String someString = partyRepository.findSomeString();
  }

  @Test
  public void shouldDoNumericProjectionWithStringBasedQuery() {
    Party partyHasKeyword = new Party(KEY_PARTY_KEYWORD, "party", "desc is a N1QL keyword", new Date(), 4000000, new Point(500, 500));
    partyRepository.save(partyHasKeyword);

    long max = partyRepository.findMaxAttendees();
    assertEquals(4000000, max);
  }

  @Test
  public void shouldDoBooleanProjectionWithStringBasedQuery() {
    boolean someBoolean = partyRepository.justABoolean();
    assertEquals(true, someBoolean);
  }
}
