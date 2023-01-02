/*
 * Copyright 2012-2023 the original author or authors
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

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.springframework.data.couchbase.core.ReactiveFindByIdOperationSupport.ReactiveFindByIdSupport;
import org.springframework.util.Assert;

import com.couchbase.client.java.kv.GetOptions;

public class ExecutableFindByIdOperationSupport implements ExecutableFindByIdOperation {

	private final CouchbaseTemplate template;

	ExecutableFindByIdOperationSupport(CouchbaseTemplate template) {
		this.template = template;
	}

	@Override
	public <T> ExecutableFindById<T> findById(Class<T> domainType) {
		return new ExecutableFindByIdSupport<>(template, domainType, null, null, null, null, null);
	}

	static class ExecutableFindByIdSupport<T> implements ExecutableFindById<T> {

		private final CouchbaseTemplate template;
		private final Class<T> domainType;
		private final String scope;
		private final String collection;
		private final GetOptions options;
		private final List<String> fields;
		private final Duration expiry;
		private final ReactiveFindByIdSupport<T> reactiveSupport;

		ExecutableFindByIdSupport(CouchbaseTemplate template, Class<T> domainType, String scope, String collection,
				GetOptions options, List<String> fields, Duration expiry) {
			this.template = template;
			this.domainType = domainType;
			this.scope = scope;
			this.collection = collection;
			this.options = options;
			this.fields = fields;
			this.expiry = expiry;
			this.reactiveSupport = new ReactiveFindByIdSupport<>(template.reactive(), domainType, scope, collection, options,
					fields, expiry, new NonReactiveSupportWrapper(template.support()));
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
		public TerminatingFindById<T> withOptions(final GetOptions options) {
			Assert.notNull(options, "Options must not be null.");
			return new ExecutableFindByIdSupport<>(template, domainType, scope, collection, options, fields, expiry);
		}

		@Override
		public FindByIdWithOptions<T> inCollection(final String collection) {
			return new ExecutableFindByIdSupport<>(template, domainType, scope, collection, options, fields, expiry);
		}

		@Override
		public FindByIdInCollection<T> inScope(final String scope) {
			return new ExecutableFindByIdSupport<>(template, domainType, scope, collection, options, fields, expiry);
		}

		@Override
		public FindByIdInScope<T> project(String... fields) {
			Assert.notEmpty(fields, "Fields must not be null.");
			return new ExecutableFindByIdSupport<>(template, domainType, scope, collection, options, Arrays.asList(fields), expiry);
		}

		@Override
		public FindByIdWithProjection<T> withExpiry(final Duration expiry) {
			return new ExecutableFindByIdSupport<>(template, domainType, scope, collection, options, fields,
					expiry);
		}

	}

}
