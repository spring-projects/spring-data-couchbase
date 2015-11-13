/*
 * Copyright 2013-2015 the original author or authors.
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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Date;
import java.util.List;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.error.DocumentDoesNotExistException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.couchbase.IntegrationTestApplicationConfig;
import org.springframework.data.couchbase.repository.config.RepositoryOperationsMapping;
import org.springframework.data.couchbase.repository.support.CouchbaseRepositoryFactory;
import org.springframework.data.couchbase.repository.support.IndexManager;
import org.springframework.data.geo.Point;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @author Simon Baslé
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = IntegrationTestApplicationConfig.class)
public class N1qlCrudRepositoryTests {

  @Autowired
  private Bucket client;

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
    partyRepository = new CouchbaseRepositoryFactory(operationsMapping, indexManager).getRepository(PartyRepository.class);
    itemRepository = new CouchbaseRepositoryFactory(operationsMapping, indexManager).getRepository(ItemRepository.class);

    itemRepository.save(item);
    partyRepository.save(party);
  }

  @After
  public void cleanUp() {
    try { itemRepository.delete(KEY_ITEM); } catch (DocumentDoesNotExistException e) {}
    try { partyRepository.delete(KEY_PARTY); } catch (DocumentDoesNotExistException e) {}
    try { partyRepository.delete(KEY_PARTY_KEYWORD); } catch (DocumentDoesNotExistException e) {}
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

    assertTrue(client.exists(KEY_PARTY_KEYWORD));
    assertTrue(parties.contains(partyHasKeyword));
    for (Object o : parties) {
      if (!(o instanceof Party)) {
        fail("expected only Party objects");
      }
    }
  }
}
