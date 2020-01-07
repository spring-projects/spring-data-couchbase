/*
 * Copyright 2012-2020 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.couchbase.repository;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.annotation.Id;

import org.springframework.data.couchbase.ContainerResourceRunner;
import org.springframework.data.couchbase.IntegrationTestApplicationConfig;
import org.springframework.data.couchbase.core.mapping.Document;
import org.springframework.data.couchbase.core.mapping.id.GeneratedValue;
import org.springframework.data.couchbase.repository.config.RepositoryOperationsMapping;
import org.springframework.data.couchbase.repository.support.CouchbaseRepositoryFactory;
import org.springframework.data.couchbase.repository.support.IndexManager;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;
import org.springframework.test.context.ContextConfiguration;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.springframework.data.couchbase.core.mapping.id.GenerationStrategy.UNIQUE;

/**
 * @author Maxence Labusquiere
 */
@RunWith(ContainerResourceRunner.class)
@ContextConfiguration(classes = IntegrationTestApplicationConfig.class)
public class CouchbaseIdGenerationIntegrationTests {

  @Autowired
  private RepositoryOperationsMapping operationsMapping;

  @Autowired
  private IndexManager indexManager;
  private CrudRepository<SimpleClassWithGeneratedIdValueUsingUUID, String> entityRepository;

  @Before
  public void setUp() throws Exception {
    entityRepository = new CouchbaseRepositoryFactory(operationsMapping, indexManager).getRepository(EntityRepository.class);
  }

  @Test
  public void idFieldEntityIsFillWithGeneratedValueOnSave() {
    SimpleClassWithGeneratedIdValueUsingUUID entity = new SimpleClassWithGeneratedIdValueUsingUUID();
    SimpleClassWithGeneratedIdValueUsingUUID savedEntity = entityRepository.save(entity);
    assertThat("Expected generated value", savedEntity.id != null);
    if (entityRepository.existsById(savedEntity.id)) {
      entityRepository.existsById(savedEntity.id);
    }
  }


  @Test
  public void ifIdFieldIsAlreadySetNothingIsDone() {
    String id = "AnId";
    SimpleClassWithGeneratedIdValueUsingUUID entity = new SimpleClassWithGeneratedIdValueUsingUUID();
    entity.setId(id);
    SimpleClassWithGeneratedIdValueUsingUUID savedEntity = entityRepository.save(entity);
    assertThat("Expected same id instance", savedEntity.id == id);
    if (entityRepository.existsById(savedEntity.id)) {
      entityRepository.existsById(savedEntity.id);
    }
  }

  @Document
  static class SimpleClassWithGeneratedIdValueUsingUUID {
    @Id
    @GeneratedValue(strategy = UNIQUE)
    public String id;

    public String value = "new";

    public void setId(String id) {
      this.id = id;
    }
  }

  @Repository
  interface EntityRepository extends CrudRepository<SimpleClassWithGeneratedIdValueUsingUUID, String> {
  }
}
