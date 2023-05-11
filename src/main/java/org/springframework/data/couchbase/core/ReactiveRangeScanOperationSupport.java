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

import reactor.core.publisher.Flux;

import java.nio.charset.StandardCharsets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.couchbase.core.query.OptionsBuilder;
import org.springframework.data.couchbase.core.support.PseudoArgs;
import org.springframework.util.Assert;

import com.couchbase.client.java.ReactiveCollection;
import com.couchbase.client.java.kv.MutationState;
import com.couchbase.client.java.kv.ScanOptions;
import com.couchbase.client.java.kv.ScanSort;
import com.couchbase.client.java.kv.ScanTerm;
import com.couchbase.client.java.kv.ScanType;

public class ReactiveRangeScanOperationSupport implements ReactiveRangeScanOperation {

	private final ReactiveCouchbaseTemplate template;
	private static final Logger LOG = LoggerFactory.getLogger(ReactiveRangeScanOperationSupport.class);

	ReactiveRangeScanOperationSupport(ReactiveCouchbaseTemplate template) {
		this.template = template;
	}

	@Override
	public <T> ReactiveRangeScan<T> rangeScan(Class<T> domainType) {
		return new ReactiveRangeScanSupport<>(template, domainType, OptionsBuilder.getScopeFrom(domainType),
				OptionsBuilder.getCollectionFrom(domainType), null, null, null, null, null, null, null, null, null,
				template.support());
	}

	static class ReactiveRangeScanSupport<T> implements ReactiveRangeScan<T> {

		private final ReactiveCouchbaseTemplate template;
		private final Class<T> domainType;
		private final String scope;
		private final String collection;
		private final ScanOptions options;
		private final Boolean isSamplingScan;
		private final ScanSort sort;
		private final MutationState mutationState;
		private final Boolean idsOnly;
		private final Long limit;
		private final Long seed;
		private final Integer batchItemLimit;
		private final Integer batchByteLimit;
		private final ReactiveTemplateSupport support;

		ReactiveRangeScanSupport(ReactiveCouchbaseTemplate template, Class<T> domainType, String scope, String collection,
				ScanOptions options, Boolean isSamplingScan, ScanSort sort, MutationState mutationState, Boolean idsOnly,
				Long limit, Long seed, Integer batchItemLimit, Integer batchByteLimit, ReactiveTemplateSupport support) {
			this.template = template;
			this.domainType = domainType;
			this.scope = scope;
			this.collection = collection;
			this.isSamplingScan = isSamplingScan;
			this.options = options;
			this.sort = sort;
			this.mutationState = mutationState;
			this.idsOnly = idsOnly;
			this.limit = limit;
			this.seed = seed;
			this.batchItemLimit = batchItemLimit;
			this.batchByteLimit = batchByteLimit;
			this.support = support;
		}

		@Override
		public TerminatingRangeScan<T> withOptions(final ScanOptions options) {
			Assert.notNull(options, "Options must not be null.");
			return new ReactiveRangeScanSupport<>(template, domainType, scope, collection, options, isSamplingScan, sort,
					mutationState, idsOnly, limit, seed, batchItemLimit, batchByteLimit, support);
		}

		@Override
		public RangeScanWithOptions<T> inCollection(final String collection) {
			return new ReactiveRangeScanSupport<>(template, domainType, scope,
					collection != null ? collection : this.collection, options, isSamplingScan, sort, mutationState, idsOnly,
					limit, seed, batchItemLimit, batchByteLimit, support);
		}

		@Override
		public RangeScanInCollection<T> inScope(final String scope) {
			return new ReactiveRangeScanSupport<>(template, domainType, scope != null ? scope : this.scope, collection,
					options, isSamplingScan, sort, mutationState, idsOnly, limit, seed, batchItemLimit, batchByteLimit, support);
		}

		@Override
		public RangeScanInScope<T> withSampling(Boolean isSamplingScan) {
			return new ReactiveRangeScanSupport<>(template, domainType, scope, collection, options, isSamplingScan, sort,
					mutationState, idsOnly, limit, seed, batchItemLimit, batchByteLimit, support);
		}

		@Override
		public RangeScanWithSampling<T> withSort(ScanSort sort) {
			return new ReactiveRangeScanSupport<>(template, domainType, scope, collection, options, isSamplingScan, sort,
					mutationState, idsOnly, limit, seed, batchItemLimit, batchByteLimit, support);
		}

		@Override
		public RangeScanWithSort<T> consistentWith(MutationState mutationState) {
			return new ReactiveRangeScanSupport<>(template, domainType, scope, collection, options, isSamplingScan, sort,
					mutationState, idsOnly, limit, seed, batchItemLimit, batchByteLimit, support);
		}

		@Override
		public <R> RangeScanConsistentWith<R> as(Class<R> returnType) {
			return new ReactiveRangeScanSupport<>(template, returnType, scope, collection, options, isSamplingScan, sort,
					mutationState, idsOnly, limit, seed, batchItemLimit, batchByteLimit, support);
		}

		@Override
		public RangeScanWithProjection<T> idsOnly(Boolean idsOnly) {
			return new ReactiveRangeScanSupport<>(template, domainType, scope, collection, options, isSamplingScan, sort,
					mutationState, idsOnly, limit, seed, batchItemLimit, batchByteLimit, support);
		}

		@Override
		public RangeScanIdsOnly<T> withLimit(Long limit) {
			return new ReactiveRangeScanSupport<>(template, domainType, scope, collection, options, isSamplingScan, sort,
					mutationState, idsOnly, limit, seed, batchItemLimit, batchByteLimit, support);
		}

		@Override
		public RangeScanWithLimit<T> withSeed(Long seed) {
			return new ReactiveRangeScanSupport<>(template, domainType, scope, collection, options, isSamplingScan, sort,
					mutationState, idsOnly, limit, seed, batchItemLimit, batchByteLimit, support);
		}

		@Override
		public RangeScanWithSeed<T> withBatchItemLimit(Integer batchItemLimit) {
			return new ReactiveRangeScanSupport<>(template, domainType, scope, collection, options, isSamplingScan, sort,
					mutationState, idsOnly, limit, seed, batchItemLimit, batchByteLimit, support);
		}

		@Override
		public RangeScanWithBatchByteLimit<T> withBatchByteLimit(Integer batchByteLimit) {
			return new ReactiveRangeScanSupport<>(template, domainType, scope, collection, options, isSamplingScan, sort,
					mutationState, idsOnly, limit, seed, batchItemLimit, batchByteLimit, support);
		}

		@Override
		public Flux<T> rangeScan(String lower, String upper) {

			PseudoArgs<ScanOptions> pArgs = new PseudoArgs<>(template, scope, collection, options, domainType);
			if (LOG.isDebugEnabled()) {
				LOG.debug("rangeScan lower={} upper={} {}", lower, upper, pArgs);
			}
			ReactiveCollection rc = template.getCouchbaseClientFactory().withScope(pArgs.getScope())
					.getCollection(pArgs.getCollection()).reactive();

			ScanTerm lowerTerm = ScanTerm.minimum();
			ScanTerm upperTerm = ScanTerm.maximum();
			if (lower != null) {
				lowerTerm = ScanTerm.inclusive(lower);
			}
			if (upper != null) {
				upperTerm = ScanTerm.inclusive(upper);
			}

			ScanType scanType = isSamplingScan ? ScanType.samplingScan(limit != null ? limit : 2, seed != null ? seed : 0)
					: ScanType.rangeScan(lowerTerm, upperTerm);
			Flux<T> reactiveEntities = TransactionalSupport.verifyNotInTransaction("rangeScan")
					.thenMany(rc.scan(scanType, buildScanOptions(pArgs.getOptions(), idsOnly))
							.flatMap(result -> support.decodeEntity(result.id(),
									new String(result.contentAsBytes(), StandardCharsets.UTF_8), result.cas(), domainType,
									pArgs.getScope(), pArgs.getCollection(), null, null)));

			return reactiveEntities.onErrorMap(throwable -> {
				if (throwable instanceof RuntimeException) {
					return template.potentiallyConvertRuntimeException((RuntimeException) throwable);
				} else {
					return throwable;
				}
			});

		}

		@Override
		public Flux<String> rangeScanIds(String lower, String upper) {
			PseudoArgs<ScanOptions> pArgs = new PseudoArgs<>(template, scope, collection, options, domainType);
			if (LOG.isDebugEnabled()) {
				LOG.debug("rangeScan lower={} upper={} {}", lower, upper, pArgs);
			}
			ReactiveCollection rc = template.getCouchbaseClientFactory().withScope(pArgs.getScope())
					.getCollection(pArgs.getCollection()).reactive();

			ScanTerm lowerTerm = ScanTerm.minimum();
			ScanTerm upperTerm = ScanTerm.maximum();
			if (lower != null) {
				lowerTerm = ScanTerm.inclusive(lower);
			}
			if (upper != null) {
				upperTerm = ScanTerm.inclusive(upper);
			}

			ScanType scanType = isSamplingScan ? ScanType.samplingScan(limit, seed)
					: ScanType.rangeScan(lowerTerm, upperTerm);
			Flux<String> reactiveEntities = TransactionalSupport.verifyNotInTransaction("rangeScanIds")
					.thenMany(rc.scan(scanType, buildScanOptions(pArgs.getOptions(), true)).map(result -> result.id()));

			return reactiveEntities.onErrorMap(throwable -> {
				if (throwable instanceof RuntimeException) {
					return template.potentiallyConvertRuntimeException((RuntimeException) throwable);
				} else {
					return throwable;
				}
			});

		}

		private ScanOptions buildScanOptions(ScanOptions options, Boolean idsOnly) {
			return OptionsBuilder.buildScanOptions(options, sort, idsOnly, mutationState, batchByteLimit, batchItemLimit);
		}
	}

}
