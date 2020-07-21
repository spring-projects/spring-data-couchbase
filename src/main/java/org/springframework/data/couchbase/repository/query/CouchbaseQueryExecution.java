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

import org.springframework.data.couchbase.core.ExecutableFindByQueryOperation;
import org.springframework.data.couchbase.core.ReactiveFindByQueryOperation;
import org.springframework.data.couchbase.core.query.Query;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.SliceImpl;
import org.springframework.data.repository.support.PageableExecutionUtils;
import org.springframework.util.Assert;

/**
 * Set of classes to contain query execution strategies. Depending (mostly) on the return type of a
 * {@link org.springframework.data.repository.query.QueryMethod} a {@link Query} can be executed in various flavors.
 *
 * @author Oliver Gierke
 * @author Mark Paluch
 * @author Christoph Strobl
 * @author Michael Reiche
 */
@FunctionalInterface
interface CouchbaseQueryExecution {

	Object execute(Query query);

	/**
	 * {@link CouchbaseQueryExecution} for {@link Slice} query methods.
	 *
	 * @author Oliver Gierke
	 * @author Christoph Strobl
	 * @since 1.5
	 */
	final class SlicedExecution implements CouchbaseQueryExecution {

		private final ExecutableFindByQueryOperation.ExecutableFindByQuery<?> find;
		private final Pageable pageable;

		public SlicedExecution(ExecutableFindByQueryOperation.ExecutableFindByQuery find, Pageable pageable) {

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
		public Object execute(Query query) {

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
	 *
	 * @author Oliver Gierke
	 * @author Mark Paluch
	 * @author Christoph Strobl
	 */
	final class PagedExecution<FindWithQuery> implements CouchbaseQueryExecution {

		private final ExecutableFindByQueryOperation.ExecutableFindByQuery<?> operation;
		private final Pageable pageable;

		public PagedExecution(ExecutableFindByQueryOperation.ExecutableFindByQuery<?> operation, Pageable pageable) {

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
		public Object execute(Query query) {

			int overallLimit = query.getLimit();

			ExecutableFindByQueryOperation.TerminatingFindByQuery<?> matching = operation.matching(query);

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

	final class ReactivePagedExecution<FindWithQuery> implements CouchbaseQueryExecution {

		private final ReactiveFindByQueryOperation.ReactiveFindByQuery<?> operation;
		private final Pageable pageable;

		public ReactivePagedExecution(ReactiveFindByQueryOperation.ReactiveFindByQuery<?> operation, Pageable pageable) {

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
		public Object execute(Query query) {

			int overallLimit = query.getLimit();

			ReactiveFindByQueryOperation.TerminatingFindByQuery<?> matching = operation.matching(query);

			// Apply raw pagination
			query.with(pageable);

			// Adjust limit if page would exceed the overall limit
			if (overallLimit != 0 && pageable.getOffset() + pageable.getPageSize() > overallLimit) {
				query.limit((int) (overallLimit - pageable.getOffset()));
			}

			return PageableExecutionUtils.getPage(matching.all().collectList().block(), pageable, () -> {

				long count = operation.matching(query.skip(-1).limit(-1)).count().block();
				return overallLimit != 0 ? Math.min(count, overallLimit) : count;
			});
		}
	}
}
