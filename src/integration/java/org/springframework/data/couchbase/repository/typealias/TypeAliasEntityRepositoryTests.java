/*
 * Copyright 2017 the original author or authors.
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

package org.springframework.data.couchbase.repository.typealias;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.view.DefaultView;
import com.couchbase.client.java.view.DesignDocument;
import com.couchbase.client.java.view.SpatialView;
import com.couchbase.client.java.view.View;
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
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.Point;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertTrue;

/**
 * The aim of this test is to ensure that each query type can use
 * the TypeAlias to retrieve an entity
 *
 * @author Maxence Labusquiere
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = IntegrationTestApplicationConfig.class)
public class TypeAliasEntityRepositoryTests {

  private static final String TYPE_ALIAS_VALUE = "MyTypeAlias";
  private static final String KEY_PARTY = "TypeAliasedParty1";

  @Autowired
  private RepositoryOperationsMapping repositoryOperationsMapping;
  @Autowired
  private IndexManager indexManager;
  @Autowired
  private Bucket client;

  private TypeAliasedPartyRepository typeAliasedPartyRepository;

  @Before
  public void setup() throws Exception {
    RepositoryFactorySupport factory = new CouchbaseRepositoryFactory(repositoryOperationsMapping, indexManager);
    client.bucketManager().upsertDesignDocument(createViewDocDesign());
    client.bucketManager().upsertDesignDocument(createGeoDocDesign());

    typeAliasedPartyRepository = factory.getRepository(TypeAliasedPartyRepository.class);
  }

  @After
  public void cleanUp() {
    try {
      typeAliasedPartyRepository.deleteById(KEY_PARTY);
    } catch (DataRetrievalFailureException e) {
    }
  }

  @Test
  public void testTypeAliasOnN1QLQuery() {
    typeAliasedPartyRepository
      .save(new TypeAliasedParty(KEY_PARTY, "partyName", "testTypeAliasOnN1QLQuery", null, 1, null));
    Iterable<TypeAliasedParty> parties = typeAliasedPartyRepository.n1qlFindParties();
    assertTrue(hasSize(1, parties));
  }

  @Test
  public void testTypeAliasOnViewQuery() {
    typeAliasedPartyRepository
      .save(new TypeAliasedParty(KEY_PARTY, "partyName", "testTypeAliasOnViewQuery", null, 1, null));
    Iterable<TypeAliasedParty> parties = typeAliasedPartyRepository.findAll();
    assertTrue(hasSize(1, parties));
  }

  @Test
  public void testTypeAliasOnDimensionalQuery() {
    final Point location = new Point(1, -2);
    typeAliasedPartyRepository
      .save(new TypeAliasedParty(KEY_PARTY, "partyName", "testTypeAliasOnDimensionalQuery", null, 1, location));
    Iterable<TypeAliasedParty> parties = typeAliasedPartyRepository.findByLocationNear(location, new Distance(10));
    assertTrue(hasSize(1, parties));
  }

  private boolean hasSize(int expectedSize, Iterable<TypeAliasedParty> parties) {
    int actualSize = 0;
    for (TypeAliasedParty ignore : parties) {
      actualSize++;
    }
    return expectedSize == actualSize;
  }

  private DesignDocument createViewDocDesign() {
    String mapFunction =
      "function (doc, meta) { if(doc._class == \"" + TYPE_ALIAS_VALUE + "\") { emit(doc.id, null); } }";
    View view = DefaultView.create("all", mapFunction, "_count");
    List<View> views = Collections.singletonList(view);
    return DesignDocument.create("typeAliasedParty", views);
  }

  private DesignDocument createGeoDocDesign() {
    String mapFunction;
    List<View> geoViews = new ArrayList<View>();
    mapFunction = "function (doc, meta) { if(doc._class == \"" + TYPE_ALIAS_VALUE + "\") "
      + "{ emit([doc.location.x, doc.location.y], null); } }";
    geoViews.add(SpatialView.create("byLocation", mapFunction));
    return DesignDocument.create("typeAliasedPartyGeo", geoViews);
  }
}
