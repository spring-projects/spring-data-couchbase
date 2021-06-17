/*
 * Copyright 2012-2021 the original author or authors
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

import static com.couchbase.client.java.kv.GetAnyReplicaOptions.getAnyReplicaOptions;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.couchbase.core.support.PseudoArgs;
import org.springframework.util.Assert;

import com.couchbase.client.java.codec.RawJsonTranscoder;
import com.couchbase.client.java.kv.GetAnyReplicaOptions;

public class ReactiveFindFromReplicasByIdOperationSupport implements ReactiveFindFromReplicasByIdOperation {

	private final ReactiveCouchbaseTemplate template;
	private static final Logger LOG = LoggerFactory.getLogger(ReactiveFindFromReplicasByIdOperationSupport.class);

	ReactiveFindFromReplicasByIdOperationSupport(ReactiveCouchbaseTemplate template) {
		this.template = template;
	}

	@Override
	public <T> ReactiveFindFromReplicasById<T> findFromReplicasById(Class<T> domainType) {
		return new ReactiveFindFromReplicasByIdSupport<>(template, domainType, domainType, null, null, null,
				template.support());
	}

	static class ReactiveFindFromReplicasByIdSupport<T> implements ReactiveFindFromReplicasById<T> {

		private final ReactiveCouchbaseTemplate template;
		private final Class<?> domainType;
		private final Class<T> returnType;
		private final String scope;
		private final String collection;
		private final GetAnyReplicaOptions options;
		private final ReactiveTemplateSupport support;

		ReactiveFindFromReplicasByIdSupport(ReactiveCouchbaseTemplate template, Class<?> domainType, Class<T> returnType,
				String scope, String collection, GetAnyReplicaOptions options, ReactiveTemplateSupport support) {
			this.template = template;
			this.domainType = domainType;
			this.returnType = returnType;
			this.scope = scope;
			this.collection = collection;
			this.options = options;
			this.support = support;
		}

		@Override
		public Mono<T> any(final String id) {
			return Mono.just(id).flatMap(docId -> {
				GetAnyReplicaOptions garOptions = options != null ? options : getAnyReplicaOptions();
				if (garOptions.build().transcoder() == null) {
					garOptions.transcoder(RawJsonTranscoder.INSTANCE);
				}
				PseudoArgs<GetAnyReplicaOptions> pArgs = new PseudoArgs<>(template, scope, collection, garOptions, domainType);
				LOG.trace("statement: {} scope: {} collection: {}", "getAnyReplica", pArgs.getScope(), pArgs.getCollection());
				return template.getCouchbaseClientFactory().withScope(pArgs.getScope()).getCollection(pArgs.getCollection())
						.reactive().getAnyReplica(docId, pArgs.getOptions());
			}).flatMap(result -> support.decodeEntity(id, result.contentAs(String.class), result.cas(), returnType))
					.onErrorMap(throwable -> {
						if (throwable instanceof RuntimeException) {
							return template.potentiallyConvertRuntimeException((RuntimeException) throwable);
						} else {
							return throwable;
						}
					});
		}

		@Override
		public Flux<? extends T> any(Collection<String> ids) {
			return Flux.fromIterable(ids).flatMap(this::any);
		}

		@Override
		public TerminatingFindFromReplicasById<T> withOptions(final GetAnyReplicaOptions options) {
			Assert.notNull(options, "Options must not be null.");
			return new ReactiveFindFromReplicasByIdSupport<>(template, domainType, returnType, scope, collection, options,
					support);
		}

		@Override
		public FindFromReplicasByIdWithOptions<T> inCollection(final String collection) {
			return new ReactiveFindFromReplicasByIdSupport<>(template, domainType, returnType, scope, collection, options,
					support);
		}

		@Override
		public FindFromReplicasByIdInCollection<T> inScope(final String scope) {
			return new ReactiveFindFromReplicasByIdSupport<>(template, domainType, returnType, scope, collection, options,
					support);
		}

	}

}
