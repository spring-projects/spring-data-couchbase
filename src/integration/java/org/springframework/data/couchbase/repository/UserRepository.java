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

import java.util.List;

import com.couchbase.client.java.view.ViewQuery;

import org.springframework.data.couchbase.core.query.Query;
import org.springframework.data.couchbase.core.query.View;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;

/**
 * @author Michael Nitschinger
 */
public interface UserRepository extends CouchbaseRepository<User, String> {

  @View(designDocument = "user", viewName = "all")
  Iterable<User> customViewQuery(ViewQuery query);

  @Query("${#n1ql.selectEntity} WHERE username = $1")
  User findByUsername(String username);

  @Query("SELECT * FROM ${#n1ql.bucket} WHERE username = $1")
  User findByUsernameBadSelect(String username);

  @Query("${#n1ql.selectEntity} WHERE username LIKE '%-${3 + 1}'")
  User findByUsernameWithSpelAndPlaceholder();

  @Query
  User findByUsernameRegexAndUsernameIn(String regex, List<String> sample);

  List<User> findByUsernameContains(String contains);

  User findByUsernameNear(String place);//this is to check that there's a N1QL derivation AND it fails

  Page<User> findByAgeGreaterThan(int minAge, Pageable pageable);

  Slice<User> findByAgeLessThan(int maxAge, Pageable pageable);
}
