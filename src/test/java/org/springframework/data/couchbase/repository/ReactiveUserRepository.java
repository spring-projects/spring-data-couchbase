/*
 * Copyright 2017-2019 the original author or authors.
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

import java.util.List;

import org.springframework.data.couchbase.core.query.N1qlSecondaryIndexed;
import org.springframework.data.couchbase.core.query.Query;
import org.springframework.data.couchbase.core.query.ViewIndexed;
import reactor.core.publisher.Flux;

/**
 * @author Subhashni Balakrishnan
 */
@ViewIndexed(designDoc = "reactiveUser", viewName = "all")
@N1qlSecondaryIndexed(indexName = "reactiveUser")
public interface ReactiveUserRepository extends ReactiveCouchbaseRepository<ReactiveUser, String> {

	@Query("#{#n1ql.selectEntity} WHERE username = $1 and #{#n1ql.filter}")
	Flux<ReactiveUser> findByUsername(String username);

	@Query("SELECT * FROM #{#n1ql.bucket} WHERE username = $1 and #{#n1ql.filter}")
	Flux<ReactiveUser> findByUsernameBadSelect(String username);

	@Query("#{#n1ql.selectEntity} WHERE username LIKE '%-#{3 + 1}' and #{#n1ql.filter}")
	Flux<ReactiveUser> findByUsernameWithSpelAndPlaceholder();

	@Query
	Flux<ReactiveUser> findByUsernameRegexAndUsernameIn(String regex, List<String> sample);

	Flux<ReactiveUser> findByUsernameContains(String contains);

	Flux<ReactiveUser> findByUsernameNear(String place);//this is to check that there's a N1QL derivation AND it fails
}
