/*
 * Copyright 2020 the original author or authors.
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
package org.springframework.data.couchbase.repository.query;

import java.util.List;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.couchbase.core.ExecutableFindByQueryOperation.TerminatingFindByQuery;
import org.springframework.data.couchbase.core.ExecutableFindByQueryOperation.ExecutableFindByQuery;
import org.springframework.data.couchbase.core.query.Query;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.SliceImpl;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.support.PageableExecutionUtils;
import org.springframework.util.Assert;

/**
 * Set of classes to contain query execution strategies. Depending (mostly) on the return type of a
 * {@link org.springframework.data.repository.query.QueryMethod} a {@link Query} can be executed in various flavors.
 *
 * @author Michael Reiche
 * @since 4.1
 */
@FunctionalInterface
interface CouchbaseQueryExecution {

	Object execute(Query query, Class<?> type, String collection);

	/**
	 * {@link CouchbaseQueryExecution} removing documents matching the query.
	 */

	final class DeleteExecution implements CouchbaseQueryExecution {

		private final CouchbaseOperations operations;
		private final QueryMethod method;

		public DeleteExecution(CouchbaseOperations operations, QueryMethod method) {
			this.operations = operations;
			this.method = method;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.couchbase.repository.query.AbstractCouchbaseQuery.Execution#execute(org.springframework.data.couchbase.core.query.Query, java.lang.Class, java.lang.String)
		 */
		@Override
		public Object execute(Query query, Class<?> type, String collection) {
			return operations.removeByQuery(type).matching(query).all();
		}

	}

	/**
	 * An {@link ReactiveCouchbaseQueryExecution} that wraps the results of the given delegate with the given result
	 * processing.
	 */
	final class ResultProcessingExecution implements CouchbaseQueryExecution {

		private final CouchbaseQueryExecution delegate;
		private final Converter<Object, Object> converter;

		public ResultProcessingExecution(CouchbaseQueryExecution delegate, Converter<Object, Object> converter) {
			Assert.notNull(delegate, "Delegate must not be null!");
			Assert.notNull(converter, "Converter must not be null!");
			this.delegate = delegate;
			this.converter = converter;
		}

		@Override
		public Object execute(Query query, Class<?> type, String collection) {
			return converter.convert(delegate.execute(query, type, collection));
		}
	}

	/**
	 * {@link CouchbaseQueryExecution} for {@link Slice} query methods.
	 */
	final class SlicedExecution implements CouchbaseQueryExecution {

		private final ExecutableFindByQuery<?> find;
		private final Pageable pageable;

		public SlicedExecution(ExecutableFindByQuery find, Pageable pageable) {
			Assert.notNull(find, "Find must not be null!");
			Assert.notNull(pageable, "Pageable must not be null!");
			this.find = find;
			this.pageable = pageable;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.couchbase.repository.query.CouchbaseQueryExecution#execute(org.springframework.data.couchbase.core.query.Query)
		 */
		@Override
		@SuppressWarnings({ "unchecked", "rawtypes" })
		public Object execute(Query query, Class<?> type, String collection) {
			int pageSize = pageable.getPageSize();
			// Apply Pageable but tweak limit to peek into next page
			Query modifiedQuery = query.with(pageable).limit(pageSize + 1);
			List result = find.matching(modifiedQuery).all();
			boolean hasNext = result.size() > pageSize;
			return new SliceImpl<Object>(hasNext ? result.subList(0, pageSize) : result, pageable, hasNext);
		}
	}

	/**
	 * {@link CouchbaseQueryExecution} for pagination queries.
	 */
	final class PagedExecution<FindWithQuery> implements CouchbaseQueryExecution {

		private final ExecutableFindByQuery<?> operation;
		private final Pageable pageable;

		public PagedExecution(ExecutableFindByQuery<?> operation, Pageable pageable) {
			Assert.notNull(operation, "Operation must not be null!");
			Assert.notNull(pageable, "Pageable must not be null!");
			this.operation = operation;
			this.pageable = pageable;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.couchbase.repository.query.CouchbaseQueryExecution#execute(org.springframework.data.couchbase.core.query.Query)
		 */
		@Override
		public Object execute(Query query, Class<?> type, String collection) {
			int overallLimit = 0; // query.getLimit();
			TerminatingFindByQuery<?> matching = operation.matching(query);
			// Apply raw pagination
			query.with(pageable);
			// Adjust limit if page would exceed the overall limit
			if (overallLimit != 0 && pageable.getOffset() + pageable.getPageSize() > overallLimit) {
				query.limit((int) (overallLimit - pageable.getOffset()));
			}
			return PageableExecutionUtils.getPage(matching.all(), pageable, () -> {
				long count = operation.matching(query.skip(-1).limit(-1)).count();
				return overallLimit != 0 ? Math.min(count, overallLimit) : count;
			});
		}
	}

}
