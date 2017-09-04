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

import org.springframework.data.couchbase.core.query.Dimensional;
import org.springframework.data.couchbase.core.query.N1qlSecondaryIndexed;
import org.springframework.data.couchbase.core.query.Query;
import org.springframework.data.couchbase.core.query.ViewIndexed;
import org.springframework.data.couchbase.repository.CouchbaseRepository;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.Point;

import java.util.List;

/**
 * @author Maxence Labusquiere
 */
@ViewIndexed(designDoc = "typeAliasedParty")
@N1qlSecondaryIndexed(indexName = "typeAliasedPartyIndex")
public interface TypeAliasedPartyRepository extends CouchbaseRepository<TypeAliasedParty, String> {

  @Query("#{#n1ql.selectEntity} WHERE #{#n1ql.filter}")
  List<TypeAliasedParty> n1qlFindParties();

  @Dimensional(designDocument = "typeAliasedPartyGeo", spatialViewName = "byLocation")
  List<TypeAliasedParty> findByLocationNear(Point p, Distance d);

}
