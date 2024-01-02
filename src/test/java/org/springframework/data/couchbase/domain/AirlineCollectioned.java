/*
 * Copyright 2012-2024 the original author or authors
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
package org.springframework.data.couchbase.domain;

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.PersistenceConstructor;
import org.springframework.data.couchbase.core.index.CompositeQueryIndex;
import org.springframework.data.couchbase.core.index.QueryIndexed;
import org.springframework.data.couchbase.core.mapping.Document;
import org.springframework.data.couchbase.repository.Collection;
import org.springframework.data.couchbase.repository.Scope;
import org.springframework.data.couchbase.util.CollectionAwareDefaultScopeIntegrationTests;

@Document
@Collection(CollectionAwareDefaultScopeIntegrationTests.otherCollection)
@Scope(CollectionAwareDefaultScopeIntegrationTests.otherScope)
public class AirlineCollectioned extends ComparableEntity {
	@Id String id;

	@QueryIndexed String name;

	String hqCountry;

	@PersistenceConstructor
	public AirlineCollectioned(String id, String name, String hqCountry) {
		this.id = id;
		this.name = name;
		this.hqCountry = hqCountry;
	}

	public String getId() {
		return id;
	}

	public String getName() {
		return name;
	}

	public String getHqCountry() {
		return hqCountry;
	}

}
