/*
 * Copyright 2017-2021 the original author or authors.
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

import org.springframework.data.couchbase.core.ReactiveCouchbaseOperations;
import org.springframework.data.couchbase.repository.query.CouchbaseEntityInformation;
import org.springframework.data.repository.NoRepositoryBean;
import org.springframework.data.repository.reactive.ReactiveSortingRepository;

/**
 * Couchbase-specific {@link ReactiveSortingRepository} implementation.
 *
 * @author Subhashni Balakrishnan
 * @author Michael Reiche
 * @since 3.0
 */
@NoRepositoryBean
public interface ReactiveCouchbaseRepository<T, ID> extends ReactiveSortingRepository<T, ID> {
	ReactiveCouchbaseOperations getOperations();

	CouchbaseEntityInformation<T, String> getEntityInformation();

}
