/*
 * Copyright 2012-present the original author or authors
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
				OptionsBuilder.getCollectionFrom(domainType), null, null, null, null, null,
				template.support());
	}

	static class ReactiveRangeScanSupport<T> implements ReactiveRangeScan<T> {

		private final ReactiveCouchbaseTemplate template;
		private final Class<T> domainType;
		private final String scope;
		private final String collection;
		private final ScanOptions options;
		private final Object sort;
		private final MutationState mutationState;
		private final Integer batchItemLimit;
		private final Integer batchByteLimit;
		private final ReactiveTemplateSupport support;

		ReactiveRangeScanSupport(ReactiveCouchbaseTemplate template, Class<T> domainType, String scope, String collection,
														 ScanOptions options, Object sort, MutationState mutationState,
														 Integer batchItemLimit, Integer batchByteLimit, ReactiveTemplateSupport support) {
			this.template = template;
			this.domainType = domainType;
			this.scope = scope;
			this.collection = collection;
			this.options = options;
			this.sort = sort;
			this.mutationState = mutationState;
			this.batchItemLimit = batchItemLimit;
			this.batchByteLimit = batchByteLimit;
			this.support = support;
		}

		@Override
		public TerminatingRangeScan<T> withOptions(final ScanOptions options) {
			Assert.notNull(options, "Options must not be null.");
			return new ReactiveRangeScanSupport<>(template, domainType, scope, collection, options, sort,
					mutationState, batchItemLimit, batchByteLimit, support);
		}

		@Override
		public RangeScanWithOptions<T> inCollection(final String collection) {
			return new ReactiveRangeScanSupport<>(template, domainType, scope,
					collection != null ? collection : this.collection, options, sort, mutationState,
				batchItemLimit, batchByteLimit, support);
		}

		@Override
		public RangeScanInCollection<T> inScope(final String scope) {
			return new ReactiveRangeScanSupport<>(template, domainType, scope != null ? scope : this.scope, collection,
					options, sort, mutationState, batchItemLimit, batchByteLimit, support);
		}

		@Override
		public RangeScanInScope<T> withSort(Object sort) {
			return new ReactiveRangeScanSupport<>(template, domainType, scope, collection, options, sort,
					mutationState, batchItemLimit, batchByteLimit, support);
		}

		@Override
		public RangeScanWithSort<T> consistentWith(MutationState mutationState) {
			return new ReactiveRangeScanSupport<>(template, domainType, scope, collection, options, sort,
					mutationState, batchItemLimit, batchByteLimit, support);
		}

		@Override
		public <R> RangeScanConsistentWith<R> as(Class<R> returnType) {
			return new ReactiveRangeScanSupport<>(template, returnType, scope, collection, options, sort,
					mutationState, batchItemLimit, batchByteLimit, support);
		}

		@Override
		public RangeScanWithProjection<T> withBatchItemLimit(Integer batchItemLimit) {
			return new ReactiveRangeScanSupport<>(template, domainType, scope, collection, options, sort,
					mutationState, batchItemLimit, batchByteLimit, support);
		}

		@Override
		public RangeScanWithBatchItemLimit<T> withBatchByteLimit(Integer batchByteLimit) {
			return new ReactiveRangeScanSupport<>(template, domainType, scope, collection, options, sort,
					mutationState, batchItemLimit, batchByteLimit, support);
		}

		@Override
		public Flux<T> rangeScan(String lower, String upper) {
			return rangeScan(lower, upper, false, null, null);
		}

		@Override
		public Flux<T> sampleScan(Long limit, Long... seed) {
			return rangeScan(null, null, true, limit,  seed!= null && seed.length > 0 ? seed[0] : null);
		}


		Flux<T> rangeScan(String lower, String upper, boolean isSamplingScan, Long limit, Long seed) {

			PseudoArgs<ScanOptions> pArgs = new PseudoArgs<>(template, scope, collection, options, domainType);
			if (LOG.isDebugEnabled()) {
				LOG.debug("rangeScan lower={} upper={} {}", lower, upper, pArgs);
			}
			ReactiveCollection rc = template.getCouchbaseClientFactory().withScope(pArgs.getScope())
					.getCollection(pArgs.getCollection()).reactive();

			ScanType scanType = null;
			if(isSamplingScan){
				scanType = ScanType.samplingScan(limit, seed != null ? seed : 0);
			} else {
				ScanTerm lowerTerm = null;
				ScanTerm upperTerm = null;
				if (lower != null) {
					lowerTerm = ScanTerm.inclusive(lower);
				}
				if (upper != null) {
					upperTerm = ScanTerm.inclusive(upper);
				}
				scanType = ScanType.rangeScan(lowerTerm, upperTerm);
			}

			Flux<T> reactiveEntities = TransactionalSupport.verifyNotInTransaction("rangeScan")
					.thenMany(rc.scan(scanType, buildScanOptions(pArgs.getOptions(), false))
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
		public Flux<String> rangeScanIds(String upper, String lower) {
			return rangeScanIds(upper, lower, false, null, null);
		}

		@Override
		public Flux<String> sampleScanIds(Long limit, Long... seed) {
			return rangeScanIds(null, null, true, limit,  seed!= null && seed.length > 0 ? seed[0] : null);
		}

		Flux<String> rangeScanIds(String lower, String upper, boolean isSamplingScan, Long limit, Long seed) {
			PseudoArgs<ScanOptions> pArgs = new PseudoArgs<>(template, scope, collection, options, domainType);
			if (LOG.isDebugEnabled()) {
				LOG.debug("rangeScan lower={} upper={} {}", lower, upper, pArgs);
			}
			ReactiveCollection rc = template.getCouchbaseClientFactory().withScope(pArgs.getScope())
					.getCollection(pArgs.getCollection()).reactive();

			ScanType scanType = null;
			if(isSamplingScan){
				scanType = ScanType.samplingScan(limit, seed != null ? seed : 0);
			} else {
				ScanTerm lowerTerm = null;
				ScanTerm upperTerm = null;
				if (lower != null) {
					lowerTerm = ScanTerm.inclusive(lower);
				}
				if (upper != null) {
					upperTerm = ScanTerm.inclusive(upper);
				}
				scanType = ScanType.rangeScan(lowerTerm, upperTerm);
			}

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
