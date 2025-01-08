/*
 * Copyright 2012-2025 the original author or authors
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
package org.springframework.data.couchbase.core.query;

import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.util.Assert;

public class AnalyticsQuery {

	private long skip;
	private int limit;
	private Sort sort = Sort.unsorted();

	public AnalyticsQuery() {}

	/**
	 * Set number of documents to skip before returning results.
	 *
	 * @param skip
	 * @return
	 */
	public AnalyticsQuery skip(long skip) {
		this.skip = skip;
		return this;
	}

	/**
	 * Limit the number of returned documents to {@code limit}.
	 *
	 * @param limit
	 * @return
	 */
	public AnalyticsQuery limit(int limit) {
		this.limit = limit;
		return this;
	}

	/**
	 * Sets the given pagination information on the {@link AnalyticsQuery} instance. Will transparently set {@code skip}
	 * and {@code limit} as well as applying the {@link Sort} instance defined with the {@link Pageable}.
	 *
	 * @param pageable
	 * @return
	 */
	public AnalyticsQuery with(final Pageable pageable) {
		if (pageable.isUnpaged()) {
			return this;
		}
		this.limit = pageable.getPageSize();
		this.skip = pageable.getOffset();
		return with(pageable.getSort());
	}

	/**
	 * Adds a {@link Sort} to the {@link AnalyticsQuery} instance.
	 *
	 * @param sort
	 * @return
	 */
	public AnalyticsQuery with(final Sort sort) {
		Assert.notNull(sort, "Sort must not be null!");
		if (sort.isUnsorted()) {
			return this;
		}
		this.sort = this.sort.and(sort);
		return this;
	}

	public void appendSkipAndLimit(final StringBuilder sb) {
		if (limit > 0) {
			sb.append(" LIMIT ").append(limit);
		}
		if (skip > 0) {
			sb.append(" OFFSET ").append(skip);
		}
	}

	public void appendSort(final StringBuilder sb) {
		if (sort.isUnsorted()) {
			return;
		}

		sb.append(" ORDER BY ");
		sort.stream().forEach(order -> {
			if (order.isIgnoreCase()) {
				throw new IllegalArgumentException(String.format("Given sort contained an Order for %s with ignore case! "
						+ "Couchbase N1QL does not support sorting ignoring case currently!", order.getProperty()));
			}
			sb.append(order.getProperty()).append(" ").append(order.isAscending() ? "ASC," : "DESC,");
		});
		sb.deleteCharAt(sb.length() - 1);
	}

}
