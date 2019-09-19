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

import java.util.List;

import org.springframework.data.couchbase.core.query.Query;

import com.couchbase.client.java.query.QueryScanConsistency;

public class ExecutableRemoveByQueryOperationSupport implements ExecutableRemoveByQueryOperation {

	private static final Query ALL_QUERY = new Query();

	private final CouchbaseTemplate template;

	public ExecutableRemoveByQueryOperationSupport(final CouchbaseTemplate template) {
		this.template = template;
	}

	@Override
	public <T> ExecutableRemoveByQuery<T> removeByQuery(Class<T> domainType) {
		return new ExecutableRemoveByQuerySupport<>(template, domainType, ALL_QUERY, QueryScanConsistency.NOT_BOUNDED);
	}

	static class ExecutableRemoveByQuerySupport<T> implements ExecutableRemoveByQuery<T> {

		private final CouchbaseTemplate template;
		private final Class<T> domainType;
		private final Query query;
		private final ReactiveRemoveByQueryOperationSupport.ReactiveRemoveByQuerySupport<T> reactiveSupport;
		private final QueryScanConsistency scanConsistency;

		ExecutableRemoveByQuerySupport(final CouchbaseTemplate template, final Class<T> domainType, final Query query,
				final QueryScanConsistency scanConsistency) {
			this.template = template;
			this.domainType = domainType;
			this.query = query;
			this.reactiveSupport = new ReactiveRemoveByQueryOperationSupport.ReactiveRemoveByQuerySupport<>(
					template.reactive(), domainType, query, scanConsistency);
			this.scanConsistency = scanConsistency;
		}

		@Override
		public List<RemoveResult> all() {
			return reactiveSupport.all().collectList().block();
		}

		@Override
		public TerminatingRemoveByQuery<T> matching(final Query query) {
			return new ExecutableRemoveByQuerySupport<>(template, domainType, query, scanConsistency);
		}

		@Override
		public RemoveByQueryWithQuery<T> consistentWith(final QueryScanConsistency scanConsistency) {
			return new ExecutableRemoveByQuerySupport<>(template, domainType, query, scanConsistency);
		}

	}

}
