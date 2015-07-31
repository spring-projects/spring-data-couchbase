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

import static org.junit.Assert.assertTrue;

import java.util.Date;
import java.util.List;

import com.couchbase.client.java.Bucket;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.couchbase.IntegrationTestApplicationConfig;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.repository.support.CouchbaseRepositoryFactory;
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
  private CouchbaseTemplate template;

  private PartyRepository partyRepository;

  @Before
  public void setup() throws Exception {
    partyRepository = new CouchbaseRepositoryFactory(template).getRepository(PartyRepository.class);
  }

  @Test
  public void shouldSaveObjectWithN1qlKeywordField() {
    Party party = new Party("partyHasKeyword", "party", "desc is a N1QL keyword", new Date(), 40);
    partyRepository.save(party);
    List<Party> parties = partyRepository.findAllByDescriptionNotNull();

    assertTrue(client.exists("partyHasKeyword"));
    assertTrue(parties.contains(party));
  }
}
