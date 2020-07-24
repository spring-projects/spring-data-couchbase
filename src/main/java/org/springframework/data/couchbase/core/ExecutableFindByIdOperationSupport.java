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
package org.springframework.data.couchbase.core;

import org.springframework.data.couchbase.core.ReactiveFindByIdOperationSupport.ReactiveFindByIdSupport;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.springframework.util.Assert;

public class ExecutableFindByIdOperationSupport implements ExecutableFindByIdOperation {

	private final CouchbaseTemplate template;

	ExecutableFindByIdOperationSupport(CouchbaseTemplate template) {
		this.template = template;
	}

	@Override
	public <T> ExecutableFindById<T> findById(Class<T> domainType) {
		return new ExecutableFindByIdSupport<>(template, domainType, null, null);
	}

	static class ExecutableFindByIdSupport<T> implements ExecutableFindById<T> {

		private final CouchbaseTemplate template;
		private final Class<T> domainType;
		private final String collection;
		private final List<String> fields;
		private final ReactiveFindByIdSupport<T> reactiveSupport;

		ExecutableFindByIdSupport(CouchbaseTemplate template, Class<T> domainType, String collection, List<String> fields) {
			this.template = template;
			this.domainType = domainType;
			this.collection = collection;
			this.fields = fields;
			this.reactiveSupport = new ReactiveFindByIdSupport<>(template.reactive(), domainType, collection, fields);
		}

		@Override
		public T one(final String id) {
			return reactiveSupport.one(id).block();
		}

		@Override
		public Collection<? extends T> all(final Collection<String> ids) {
			return reactiveSupport.all(ids).collectList().block();
		}

		@Override
		public TerminatingFindById<T> inCollection(final String collection) {
			Assert.hasText(collection, "Collection must not be null nor empty.");
			return new ExecutableFindByIdSupport<>(template, domainType, collection, fields);
		}

		@Override
		public FindByIdWithCollection<T> project(String... fields) {
			Assert.notEmpty(fields, "Fields must not be null nor empty.");
			return new ExecutableFindByIdSupport<>(template, domainType, collection, Arrays.asList(fields));
		}
	}

}
