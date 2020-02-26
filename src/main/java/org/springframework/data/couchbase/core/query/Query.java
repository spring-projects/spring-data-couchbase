package org.springframework.data.couchbase.core.query;

import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.util.Assert;

public class Query {

	private long skip;
	private int limit;
	private Sort sort = Sort.unsorted();

	public Query() {}

	/**
	 * Set number of documents to skip before returning results.
	 *
	 * @param skip
	 * @return
	 */
	public Query skip(long skip) {
		this.skip = skip;
		return this;
	}

	/**
	 * Limit the number of returned documents to {@code limit}.
	 *
	 * @param limit
	 * @return
	 */
	public Query limit(int limit) {
		this.limit = limit;
		return this;
	}

	/**
	 * Sets the given pagination information on the {@link Query} instance. Will transparently set {@code skip} and
	 * {@code limit} as well as applying the {@link Sort} instance defined with the {@link Pageable}.
	 *
	 * @param pageable
	 * @return
	 */
	public Query with(final Pageable pageable) {
		if (pageable.isUnpaged()) {
			return this;
		}
		this.limit = pageable.getPageSize();
		this.skip = pageable.getOffset();
		return with(pageable.getSort());
	}

	/**
	 * Adds a {@link Sort} to the {@link Query} instance.
	 *
	 * @param sort
	 * @return
	 */
	public Query with(final Sort sort) {
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
