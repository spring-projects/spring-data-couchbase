/*
 * Copyright 2012-2021 the original author or authors
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

import java.util.List;

import org.springframework.data.couchbase.core.ReactiveRemoveByQueryOperationSupport.ReactiveRemoveByQuerySupport;
import org.springframework.data.couchbase.core.query.Query;
import org.springframework.data.couchbase.transaction.CouchbaseStuffHandle;
import org.springframework.util.Assert;

import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryScanConsistency;

public class ExecutableRemoveByQueryOperationSupport implements ExecutableRemoveByQueryOperation {

	private static final Query ALL_QUERY = new Query();

	private final CouchbaseTemplate template;

	public ExecutableRemoveByQueryOperationSupport(final CouchbaseTemplate template) {
		this.template = template;
	}

	@Override
	public <T> ExecutableRemoveByQuery<T> removeByQuery(Class<T> domainType) {
		return new ExecutableRemoveByQuerySupport<>(template, domainType, ALL_QUERY, null, null, null, null, null);
	}

	static class ExecutableRemoveByQuerySupport<T> implements ExecutableRemoveByQuery<T> {

		private final CouchbaseTemplate template;
		private final Class<T> domainType;
		private final Query query;
		private final ReactiveRemoveByQuerySupport<T> reactiveSupport;
		private final QueryScanConsistency scanConsistency;
		private final String scope;
		private final String collection;
		private final QueryOptions options;
		private final CouchbaseStuffHandle txCtx;

		ExecutableRemoveByQuerySupport(final CouchbaseTemplate template, final Class<T> domainType, final Query query,
																	 final QueryScanConsistency scanConsistency, String scope, String collection, QueryOptions options,
																	 CouchbaseStuffHandle txCtx) {
			this.template = template;
			this.domainType = domainType;
			this.query = query;
			this.reactiveSupport = new ReactiveRemoveByQuerySupport<>(template.reactive(), domainType, query, scanConsistency,
					scope, collection, options, txCtx);
			this.scanConsistency = scanConsistency;
			this.scope = scope;
			this.collection = collection;
			this.options = options;
			this.txCtx = txCtx;
		}

		@Override
		public List<RemoveResult> all() {
			return reactiveSupport.all().collectList().block();
		}

		@Override
		public RemoveByQueryWithTxOrNot<T> matching(final Query query) {
			return new ExecutableRemoveByQuerySupport<>(template, domainType, query, scanConsistency, scope, collection,
					options, txCtx);
		}

		@Override
		@Deprecated
		public RemoveByQueryInScope<T> consistentWith(final QueryScanConsistency scanConsistency) {
			return new ExecutableRemoveByQuerySupport<>(template, domainType, query, scanConsistency, scope, collection,
					options, txCtx);
		}

		@Override
		public RemoveByQueryConsistentWith<T> withConsistency(final QueryScanConsistency scanConsistency) {
			return new ExecutableRemoveByQuerySupport<>(template, domainType, query, scanConsistency, scope, collection,
					options, txCtx);
		}

		@Override
		public RemoveByQueryWithQuery<T> inCollection(final String collection) {
			return new ExecutableRemoveByQuerySupport<>(template, domainType, query, scanConsistency, scope, collection,
					options, txCtx);
		}

		@Override
		public RemoveByQueryWithQuery<T> withOptions(final QueryOptions options) {
			Assert.notNull(options, "Options must not be null.");
			return new ExecutableRemoveByQuerySupport<>(template, domainType, query, scanConsistency, scope, collection,
					options, txCtx);
		}

		@Override
		public RemoveByQueryInCollection<T> inScope(final String scope) {
			return new ExecutableRemoveByQuerySupport<>(template, domainType, query, scanConsistency, scope, collection,
					options, txCtx);
		}

		@Override
		public TerminatingRemoveByQuery<T> transaction(final CouchbaseStuffHandle txCtx) {
			return new ExecutableRemoveByQuerySupport<>(template, domainType, query, scanConsistency, scope, collection,
					options, txCtx);
		}

	}

}
