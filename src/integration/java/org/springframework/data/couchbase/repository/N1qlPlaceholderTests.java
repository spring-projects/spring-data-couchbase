/*
 * Copyright 2013-2015 the original author or authors.
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

import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.couchbase.IntegrationTestApplicationConfig;
import org.springframework.data.couchbase.core.query.Query;
import org.springframework.data.couchbase.repository.config.RepositoryOperationsMapping;
import org.springframework.data.couchbase.repository.support.CouchbaseRepositoryFactory;
import org.springframework.data.couchbase.repository.support.IndexManager;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @author Simon Baslé
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = IntegrationTestApplicationConfig.class)
@TestExecutionListeners(PartyPopulatorListener.class)
public class N1qlPlaceholderTests {

  @Autowired
  private RepositoryOperationsMapping operationsMapping;

  @Autowired
  private IndexManager indexManager;

  private CouchbaseRepositoryFactory factory;
  private PartyRepository partyRepository;

  @Before
  public void setup() throws Exception {
    factory = new CouchbaseRepositoryFactory(operationsMapping, indexManager);
    partyRepository = factory.getRepository(PartyRepository.class);
  }

  @Test
  public void shouldFindUsingNamedParameters() {
    String included = "90";
    String excluded = "New Year";
    int min = 200;
    List<Party> result = partyRepository.findAllWithNamedParams(excluded, included, min);

    assertEquals(2, result.size());
    for (Party party : result) {
      assertTrue(party.getDescription().contains(included));
      assertFalse(party.getDescription().contains(excluded));
      assertTrue(party.getAttendees() >= min);
    }
  }

  @Test
  public void shouldFindUsingPositionalParameters() {
    String included = "90";
    String excluded = "New Year";
    int min = 200;
    List<Party> result = partyRepository.findAllWithPositionalParams(excluded, included, min);

    assertEquals(2, result.size());
    for (Party party : result) {
      assertTrue(party.getDescription().contains(included));
      assertFalse(party.getDescription().contains(excluded));
      assertTrue(party.getAttendees() >= min);
    }
  }

  @Test
  public void shouldIgnoreQuotedNamedParamsAndParamAnnotationIfPosUsed() {
    String included = "90";
    String excluded = "New Year";
    int min = 200;
    List<Party> result = partyRepository.findAllWithPositionalParamsAndQuotedNamedParams(excluded, included, min);

    assertEquals(2, result.size());
    for (Party party : result) {
      assertTrue(party.getDescription().contains(included));
      assertFalse(party.getDescription().contains(excluded));
      assertTrue(party.getAttendees() >= min);
    }
  }

  private interface BadRepository extends CrudRepository<Party, String> {
    @Query("#{#n1ql.selectEntity} WHERE #{#n1ql.filter} AND `desc` LIKE '%' || $2 ||  '%' AND attendees >= $3" +
        " AND `desc` NOT LIKE '%' || $included || '%'")
    List<Party> findAllWithMixedParamsInQuery(@Param("excluded") String ex, @Param("included") String inc, @Param("min") long min);
  }

  @Test
  public void shouldFailUsingMixedParameters() {
    try {
      factory.getRepository(BadRepository.class);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertEquals(e.toString(), "Using both named (1) and positional (2) placeholders is not supported, please choose " +
          "one over the other in findAllWithMixedParamsInQuery", e.getMessage());
    }
  }
}
