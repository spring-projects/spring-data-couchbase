/*
 * Copyright 2012-2022 the original author or authors
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

import java.util.stream.Stream;

import org.springframework.data.couchbase.core.ReactiveRangeScanOperationSupport.ReactiveRangeScanSupport;
import org.springframework.data.couchbase.core.query.OptionsBuilder;
import org.springframework.util.Assert;

import com.couchbase.client.java.kv.MutationState;
import com.couchbase.client.java.kv.ScanOptions;
import com.couchbase.client.java.kv.ScanSort;

public class ExecutableRangeScanOperationSupport implements ExecutableRangeScanOperation {

	private final CouchbaseTemplate template;

	ExecutableRangeScanOperationSupport(CouchbaseTemplate template) {
		this.template = template;
	}

	@Override
	public <T> ExecutableRangeScan<T> rangeScan(Class<T> domainType) {
		return new ExecutableRangeScanSupport<>(template, domainType, OptionsBuilder.getScopeFrom(domainType),
				OptionsBuilder.getCollectionFrom(domainType), null, null, null, null, null, null, null, null, null);
	}

	static class ExecutableRangeScanSupport<T> implements ExecutableRangeScan<T> {

		private final CouchbaseTemplate template;
		private final Class<T> domainType;
		private final String scope;
		private final String collection;
		private final ScanOptions options;
		private final Boolean isSamplingScan;
		private final ScanSort sort;
		private final MutationState mutationState;
		private final Boolean withContent;
		private final Long limit;
		private final Long seed;
		private final Integer batchItemLimit;
		private final Integer batchByteLimit;
		private final ReactiveRangeScanSupport<T> reactiveSupport;

		ExecutableRangeScanSupport(CouchbaseTemplate template, Class<T> domainType, String scope, String collection,
				ScanOptions options, Boolean isSamplingScan, ScanSort sort, MutationState mutationState, Boolean withContent,
				Long seed, Long limit, Integer batchItemLimit, Integer batchByteLimit) {
			this.template = template;
			this.domainType = domainType;
			this.scope = scope;
			this.collection = collection;
			this.options = options;
			this.isSamplingScan = isSamplingScan;
			this.sort = sort;
			this.mutationState = mutationState;
			this.withContent = withContent;
			this.limit = limit;
			this.seed = seed;
			this.batchItemLimit = batchItemLimit;
			this.batchByteLimit = batchByteLimit;
			this.reactiveSupport = new ReactiveRangeScanSupport<>(template.reactive(), domainType, scope, collection, options,
					isSamplingScan, sort, mutationState, withContent, limit, seed, batchItemLimit, batchByteLimit,
					new NonReactiveSupportWrapper(template.support()));
		}

		@Override
		public TerminatingRangeScan<T> withOptions(final ScanOptions options) {
			Assert.notNull(options, "Options must not be null.");
			return new ExecutableRangeScanSupport<>(template, domainType, scope, collection, options, isSamplingScan, sort,
					mutationState, withContent, limit, seed, batchItemLimit, batchByteLimit);
		}

		@Override
		public RangeScanWithOptions<T> inCollection(final String collection) {
			return new ExecutableRangeScanSupport<>(template, domainType, scope,
					collection != null ? collection : this.collection, options, isSamplingScan, sort, mutationState, withContent,
					limit, seed, batchItemLimit, batchByteLimit);
		}

		@Override
		public RangeScanInCollection<T> inScope(final String scope) {
			return new ExecutableRangeScanSupport<>(template, domainType, scope != null ? scope : this.scope, collection,
					options, isSamplingScan, sort, mutationState, withContent, limit, seed, batchItemLimit, batchByteLimit);
		}

		@Override
		public RangeScanInScope<T> withSampling(Boolean isSamplingScan) {
			return new ExecutableRangeScanSupport<>(template, domainType, scope, collection, options, isSamplingScan, sort,
					mutationState, withContent, limit, seed, batchItemLimit, batchByteLimit);
		}

		@Override
		public RangeScanWithSampling<T> withSort(ScanSort sort) {
			return new ExecutableRangeScanSupport<>(template, domainType, scope, collection, options, isSamplingScan, sort,
					mutationState, withContent, limit, seed, batchItemLimit, batchByteLimit);
		}

		@Override
		public RangeScanWithSort<T> consistentWith(MutationState mutationState) {
			return new ExecutableRangeScanSupport<>(template, domainType, scope, collection, options, isSamplingScan, sort,
					mutationState, withContent, limit, seed, batchItemLimit, batchByteLimit);
		}

		@Override
		public <R> RangeScanConsistentWith<R> as(Class<R> returnType) {
			return new ExecutableRangeScanSupport<>(template, returnType, scope, collection, options, isSamplingScan, sort,
					mutationState, withContent, limit, seed, batchItemLimit, batchByteLimit);
		}

		@Override
		public RangeScanWithProjection<T> idsOnly(Boolean withContent) {
			return new ExecutableRangeScanSupport<>(template, domainType, scope, collection, options, isSamplingScan, sort,
					mutationState, withContent, limit, seed, batchItemLimit, batchByteLimit);
		}

		@Override
		public RangeScanIdsOnly<T> withLimit(Long limit) {
			return new ExecutableRangeScanSupport<>(template, domainType, scope, collection, options, isSamplingScan, sort,
				mutationState, withContent, limit, seed, batchItemLimit, batchByteLimit);
		}

		@Override
		public RangeScanWithLimit<T> withSeed(Long seed) {
			return new ExecutableRangeScanSupport<>(template, domainType, scope, collection, options, isSamplingScan, sort,
				mutationState, withContent, limit, seed, batchItemLimit, batchByteLimit);
		}

		@Override
		public RangeScanWithSeed<T> withBatchItemLimit(Integer batchItemLimit) {
			return new ExecutableRangeScanSupport<>(template, domainType, scope, collection, options, isSamplingScan, sort,
					mutationState, withContent, limit, seed, batchItemLimit, batchByteLimit);
		}

		@Override
		public RangeScanWithBatchByteLimit<T> withBatchByteLimit(Integer batchByteLimit) {
			return new ExecutableRangeScanSupport<>(template, domainType, scope, collection, options, isSamplingScan, sort,
					mutationState, withContent, limit, seed, batchItemLimit, batchByteLimit);
		}

		@Override
		public Stream<T> rangeScan(String lower, String upper) {
			return reactiveSupport.rangeScan(lower, upper).toStream();
		}

		@Override
		public Stream<String> rangeScanIds(String lower, String upper) {
			return reactiveSupport.rangeScanIds(lower, upper).toStream();
		}

	}

}
