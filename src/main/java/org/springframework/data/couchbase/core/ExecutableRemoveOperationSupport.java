/*
 * Copyright 2017-2020 the original author or authors.
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
package org.springframework.data.couchbase.core;

import java.util.Collection;
import java.util.List;

import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.java.kv.PersistTo;
import com.couchbase.client.java.kv.ReplicateTo;
import com.couchbase.client.java.query.QueryScanConsistency;
import org.springframework.data.couchbase.core.query.Query;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;


/**
 * Implementation of {@link ExecutableRemoveOperation}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.0
 */
class ExecutableRemoveOperationSupport implements ExecutableRemoveOperation {

	private static final Query ALL_QUERY = new Query();

	private final CouchbaseTemplate template;

	public ExecutableRemoveOperationSupport(CouchbaseTemplate template) {
		this.template = template;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ExecutableRemoveOperation#remove(java.lang.Class)
	 */
	@Override
	public <T> ExecutableRemove<T> remove(Class<T> domainType) {

		Assert.notNull(domainType, "DomainType must not be null!");

		return new ExecutableRemoveSupport<>(template, domainType, ALL_QUERY, QueryScanConsistency.NOT_BOUNDED, null);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ExecutableRemoveOperation#remove(java.lang.Class)
	 */
	@Override
	public <T> ExecutableRemove<T> removeById(Class<T> domainType) {
		return new ExecutableRemoveSupport<>(template, domainType, ALL_QUERY, QueryScanConsistency.NOT_BOUNDED, null);
	}

	/**
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	static class ExecutableRemoveSupport<T> implements ExecutableRemove<T>, RemoveWithCollection<T> {

		private final CouchbaseTemplate template;
		private final Class<T> domainType;
		private final Query query;
		private final String collection;
		private final QueryScanConsistency scanConsistency;
		private final ReactiveRemoveByQueryOperationSupport.ReactiveRemoveByQuerySupport<T> reactiveSupport;


		public ExecutableRemoveSupport(CouchbaseTemplate template, Class<T> domainType, Query query, QueryScanConsistency scanConsistency, String collection) {
			this.template = template;
			this.domainType = domainType;
			this.query = query;
			this.scanConsistency = scanConsistency;
			this.collection = collection;
			this.reactiveSupport = new ReactiveRemoveByQueryOperationSupport.ReactiveRemoveByQuerySupport<T>(template.reactive(), (Class <T>)domainType, query, scanConsistency , null/* scope */, collection,
					PersistTo.NONE, ReplicateTo.NONE,
					DurabilityLevel.NONE);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.ExecutableRemoveOperation.RemoveWithCollection#inCollection(java.lang.String)
		 */
		@Override
		public RemoveWithCollection<T> inCollection(String collection) {

			Assert.hasText(collection, "Collection must not be null nor empty!");

			return new ExecutableRemoveSupport<>(template, domainType, query, scanConsistency, collection);
		}

		@Override
		public RemoveWithQuery<T> consistentWith(final QueryScanConsistency scanConsistency) {
			return new ExecutableRemoveSupport<>(template, domainType, query, scanConsistency, collection);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.ExecutableRemoveOperation.RemoveWithQuery#matching(org.springframework.data.mongodb.core.query.Query)
		 */
		@Override
		public TerminatingRemove<T> matching(Query query) {

			Assert.notNull(query, "Query must not be null!");

			return new ExecutableRemoveSupport<>(template, domainType, query, scanConsistency, collection);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.ExecutableRemoveOperation.TerminatingRemove#all()
		 */
		@Override
		public List<RemoveResult> all() {
			return reactiveSupport.all().collectList().block();
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.ExecutableRemoveOperation.TerminatingRemove#all()
		 */
		@Override
		public List<RemoveResult> all(Collection<String> ids) {
				return reactiveSupport.all(ids).collectList().block();
			}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.ExecutableRemoveOperation.TerminatingRemove#one()
		 */
		@Override
		public RemoveResult one() {
			//return template.doRemove(getCollectionName(), query, domainType, false);
			throw new RuntimeException(" not implemented");
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.ExecutableRemoveOperation.TerminatingRemove#one()
		 */
		@Override
		public RemoveResult one(String id) {
			//return template.doRemove(getCollectionName(), query, domainType, false);
			return reactiveSupport.one(id).block();
		}
		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.ExecutableRemoveOperation.TerminatingRemove#findAndRemove()
		 */
		@Override
		public List<T> findAndRemove() {

			//String collectionName = getCollectionName();

			return null; // TODO template.doFindAndDelete(collectionName, query, domainType);
		}

		//private String getCollectionName() {
		//	return StringUtils.hasText(collection) ? collection : template.getCollectionName(domainType);
		//}
	}
}
