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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.springframework.data.couchbase.CouchbaseTestHelper.getRepositoryWithRetry;

import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.couchbase.ContainerResourceRunner;
import org.springframework.data.couchbase.IntegrationTestApplicationConfig;
import org.springframework.data.couchbase.core.query.Query;
import org.springframework.data.couchbase.repository.config.RepositoryOperationsMapping;
import org.springframework.data.couchbase.repository.support.CouchbaseRepositoryFactory;
import org.springframework.data.couchbase.repository.support.IndexManager;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;

/**
 * @author Simon Baslé
 */
@RunWith(ContainerResourceRunner.class)
@ContextConfiguration(classes = IntegrationTestApplicationConfig.class)
@TestExecutionListeners(PartyPopulatorListener.class)
public class N1qlPlaceholderIntegrationTests {

  @Autowired
  private RepositoryOperationsMapping operationsMapping;

  @Autowired
  private IndexManager indexManager;

  private CouchbaseRepositoryFactory factory;
  private PartyRepository partyRepository;

  @Before
  public void setup() throws Exception {
    factory = new CouchbaseRepositoryFactory(operationsMapping, indexManager);
    partyRepository = getRepositoryWithRetry(factory, PartyRepository.class);
  }

  @Test
  public void shouldFindUsingNamedParameters() {
    String included = "90";
    String excluded = "New Year";
    int min = 200;
    List<Party> result = partyRepository.findAllWithNamedParams(excluded, included, min);

    assertThat(result.size()).isEqualTo(2);
    for (Party party : result) {
      assertThat(party.getDescription().contains(included)).isTrue();
      assertThat(party.getDescription().contains(excluded)).isFalse();
      assertThat(party.getAttendees() >= min).isTrue();
    }
  }

  @Test
  public void shouldFindUsingPositionalParameters() {
    String included = "90";
    String excluded = "New Year";
    int min = 200;
    List<Party> result = partyRepository.findAllWithPositionalParams(excluded, included, min);

    assertThat(result.size()).isEqualTo(2);
    for (Party party : result) {
      assertThat(party.getDescription().contains(included)).isTrue();
      assertThat(party.getDescription().contains(excluded)).isFalse();
      assertThat(party.getAttendees() >= min).isTrue();
    }
  }

  @Test
  public void shouldIgnoreQuotedNamedParamsAndParamAnnotationIfPosUsed() {
    String included = "90";
    String excluded = "New Year";
    int min = 200;
    List<Party> result = partyRepository.findAllWithPositionalParamsAndQuotedNamedParams(excluded, included, min);

    assertThat(result.size()).isEqualTo(2);
    for (Party party : result) {
      assertThat(party.getDescription().contains(included)).isTrue();
      assertThat(party.getDescription().contains(excluded)).isFalse();
      assertThat(party.getAttendees() >= min).isTrue();
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
      assertThat(e.getMessage()).as(e.toString())
			  .isEqualTo("Using both named (1) and positional (2) placeholders is not supported, please choose " +
					  "one over the other in findAllWithMixedParamsInQuery");
    }
  }

  @Test
  public void deleteQueryTest() {
    String included = "90";
    String excluded = "New Year";
    int max = 200;
    List<Party> result = partyRepository.removeWithPositionalParams(excluded, included, max);

    assertThat(result.size()).isEqualTo(10);
    for (Party party : result) {
      assertThat(party.getDescription().contains(included)).isTrue();
      assertThat(party.getDescription().contains(excluded)).isFalse();
      assertThat(party.getAttendees() < max).isTrue();
    }
  }
}
