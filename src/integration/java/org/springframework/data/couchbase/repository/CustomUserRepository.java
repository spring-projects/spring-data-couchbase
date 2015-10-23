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

import java.util.List;

import com.couchbase.client.java.document.json.JsonObject;

import org.springframework.data.couchbase.core.query.View;

/**
 * @author David Harrigan
 * @author Simon Baslé
 */
public interface CustomUserRepository extends CouchbaseRepository<User, String> {

  @Override
  @View(designDocument = "user", viewName = "customFindAllView")
  Iterable<User> findAll();

  @Override
  @View(designDocument = "userCustom", viewName = "customCountView")
  long count();

  @View(viewName = "allSomething")
  Iterable<User> findIncorrectExplicitView();

  @View(viewName = "customFindAllView")
  Iterable<User> findRandomMethodName();

  @View(viewName = "customFindByNameView")
  long countByUsernameGreaterThanEqualAndUsernameLessThan(String lowBound, String highBound);

  @View(viewName = "customFindByNameView")
  User findByUsernameIs(String lowKey);

  @View(viewName = "customFindByNameView")
  List<User> findAllByUsernameIn(List<String> keys);

  @View(viewName = "customFindByNameView")
  List<User> findByUsernameGreaterThanEqualAndUsernameLessThanEqual(String lowKey, String highKey);

  @View(viewName = "customFindByNameView")
  List<User> findByUsernameBetween(String lowKey, String highKey);

  @View(viewName = "customFindByNameView")
  List<User> findTop3ByUsernameGreaterThanEqual(String lowKey);

  @View(viewName = "customFindAllView")
  List<User> findAllByUsername();

  @View(viewName = "customFindAllView")
  List<User> findAllByUsernameEqualAndUserblablaIs(String s, String blabla);

  @View
  List<User> findByIncorrectView();

  @View
  long countCustomFindAllView();

  @View
  long countCustomFindInvalid();

  @View(viewName = "customFindByAgeStatsView", reduce = true)
  JsonObject findByAgeLessThan(int maxAge);
}
