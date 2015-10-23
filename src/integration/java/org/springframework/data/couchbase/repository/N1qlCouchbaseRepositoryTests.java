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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.couchbase.IntegrationTestApplicationConfig;
import org.springframework.data.couchbase.repository.config.RepositoryOperationsMapping;
import org.springframework.data.couchbase.repository.support.CouchbaseRepositoryFactory;
import org.springframework.data.couchbase.repository.support.IndexManager;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

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

  @Before
  public void setup() throws Exception {
    RepositoryFactorySupport factory = new CouchbaseRepositoryFactory(operationsMapping, indexManager);
    repository = factory.getRepository(PartyPagingRepository.class);
  }

  @Test
  public void shouldFindAllWithSort() {
    Iterable<Party> allByAttendanceDesc = repository.findAll(new Sort(Sort.Direction.DESC, "attendees"));
    long previousAttendance = Long.MAX_VALUE;
    for (Party party : allByAttendanceDesc) {
      assertTrue(party.getAttendees() <= previousAttendance);
      previousAttendance = party.getAttendees();
    }
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
  }

  @Test
  public void shouldPageThroughEntities() {
    Pageable pageable = new PageRequest(0, 8);

    Page<Party> page1 = repository.findAll(pageable);
    assertEquals(14, page1.getTotalElements()); //13 generated parties + 1 specifically crafted party
    assertEquals(8, page1.getNumberOfElements());
  }
}
