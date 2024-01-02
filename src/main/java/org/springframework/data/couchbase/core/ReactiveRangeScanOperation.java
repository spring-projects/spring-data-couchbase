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
package org.springframework.data.couchbase.core;

import reactor.core.publisher.Flux;

import org.springframework.data.couchbase.core.support.ConsistentWith;
import org.springframework.data.couchbase.core.support.InCollection;
import org.springframework.data.couchbase.core.support.InScope;
import org.springframework.data.couchbase.core.support.WithBatchByteLimit;
import org.springframework.data.couchbase.core.support.WithBatchItemLimit;
import org.springframework.data.couchbase.core.support.WithScanOptions;
import org.springframework.data.couchbase.core.support.WithScanSort;

import com.couchbase.client.java.kv.MutationState;
import com.couchbase.client.java.kv.ScanOptions;

/**
 * Get Operations
 *
 * @author Christoph Strobl
 * @since 2.0
 */
public interface ReactiveRangeScanOperation {

	/**
	 * Loads a document from a bucket.
	 *
	 * @param domainType the entity type to use for the results.
	 */
	<T> ReactiveRangeScan<T> rangeScan(Class<T> domainType);

	/**
	 * Terminating operations invoking the actual execution.
	 *
	 * @param <T> the entity type to use for the results.
	 */
	interface TerminatingRangeScan<T> /*extends OneAndAllId<T>*/ {

		/**
		 * Finds a list of documents based on the given IDs.
		 *
		 * @param lower the lower bound
		 * @param upper the upper bound
		 * @return the list of found entities.
		 */
		Flux<T> rangeScan(String lower, String upper);

		/**
		 * Finds a list of documents based on the given IDs.
		 *
		 * @param lower the lower bound
		 * @param upper the upper bound
		 * @return the list of ids.
		 */
		Flux<String> rangeScanIds(String lower, String upper);

		/**
		 * Finds a list of documents based on the given IDs.
		 *
		 * @param limit
		 * @param seed
		 * @return the list of found entities.
		 */
		Flux<T> sampleScan(Long limit, Long... seed);

		/**
		 * Finds a list of documents based on the given IDs.
		 *
		 * @param limit
		 * @param seed
		 * @return the list of ids.
		 */
		Flux<String> sampleScanIds(Long limit, Long... seed);
	}

	/**
	 * Fluent method to specify options.
	 *
	 * @param <T> the entity type to use for the results.
	 */
	interface RangeScanWithOptions<T> extends TerminatingRangeScan<T>, WithScanOptions<T> {
		/**
		 * Fluent method to specify options to use for execution
		 *
		 * @param options options to use for execution
		 */
		@Override
		TerminatingRangeScan<T> withOptions(ScanOptions options);
	}

	/**
	 * Fluent method to specify the collection.
	 *
	 * @param <T> the entity type to use for the results.
	 */
	interface RangeScanInCollection<T> extends RangeScanWithOptions<T>, InCollection<T> {
		/**
		 * With a different collection
		 *
		 * @param collection the collection to use.
		 */
		@Override
		RangeScanWithOptions<T> inCollection(String collection);
	}

	/**
	 * Fluent method to specify the scope.
	 *
	 * @param <T> the entity type to use for the results.
	 */
	interface RangeScanInScope<T> extends RangeScanInCollection<T>, InScope<T> {
		/**
		 * With a different scope
		 *
		 * @param scope the scope to use.
		 */
		@Override
		RangeScanInCollection<T> inScope(String scope);
	}

	interface RangeScanWithSort<T> extends RangeScanInScope<T>, WithScanSort<T> {
		/**
		 * sort
		 *
		 * @param sort
		 */
		@Override
		RangeScanInScope<T> withSort(Object sort);
	}

	/**
	 * Fluent method to specify scan consistency. Scan consistency may also come from an annotation.
	 *
	 * @param <T> the entity type to use for the results.
	 */
	interface RangeScanConsistentWith<T> extends RangeScanWithSort<T>, ConsistentWith<T> {

		/**
		 * Allows to override the default scan consistency.
		 *
		 * @param mutationState the custom scan consistency to use for this query.
		 */
		@Override
		RangeScanWithSort<T> consistentWith(MutationState mutationState);
	}

	/**
	 * Fluent method to specify a return type different than the the entity type to use for the results.
	 *
	 * @param <T> the entity type to use for the results.
	 */
	interface RangeScanWithProjection<T> extends RangeScanConsistentWith<T> {

		/**
		 * Define the target type fields should be mapped to. <br />
		 * Skip this step if you are only interested in the original the entity type to use for the results.
		 *
		 * @param returnType must not be {@literal null}.
		 * @return new instance of {@link ReactiveFindByQueryOperation.FindByQueryWithProjection}.
		 * @throws IllegalArgumentException if returnType is {@literal null}.
		 */
		<R> RangeScanConsistentWith<R> as(Class<R> returnType);
	}

	interface RangeScanWithBatchItemLimit<T> extends RangeScanWithProjection<T>, WithBatchItemLimit<T> {

		/**
		 * determines if result are just ids or ids plus contents
		 *
		 * @param batchByteLimit must not be {@literal null}.
		 * @return new instance of {@link RangeScanWithProjection}.
		 * @throws IllegalArgumentException if returnType is {@literal null}.
		 */
		@Override
		RangeScanWithProjection<T> withBatchItemLimit(Integer batchByteLimit);
	}

	interface RangeScanWithBatchByteLimit<T> extends RangeScanWithBatchItemLimit<T>, WithBatchByteLimit<T> {

		/**
		 * determines if result are just ids or ids plus contents
		 *
		 * @param batchByteLimit must not be {@literal null}.
		 * @return new instance of {@link RangeScanWithProjection}.
		 * @throws IllegalArgumentException if returnType is {@literal null}.
		 */
		@Override
		RangeScanWithBatchItemLimit<T> withBatchByteLimit(Integer batchByteLimit);
	}

	/**
	 * Provides methods for constructing query operations in a fluent way.
	 *
	 * @param <T> the entity type to use for the results
	 */
	interface ReactiveRangeScan<T> extends RangeScanWithBatchByteLimit<T> {}

}
