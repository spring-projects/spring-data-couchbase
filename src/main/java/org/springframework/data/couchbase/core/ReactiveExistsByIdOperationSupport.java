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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.util.Collection;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.couchbase.core.query.OptionsBuilder;
import org.springframework.data.couchbase.core.support.PseudoArgs;
import org.springframework.util.Assert;

import com.couchbase.client.java.kv.ExistsOptions;
import com.couchbase.client.java.kv.ExistsResult;

public class ReactiveExistsByIdOperationSupport implements ReactiveExistsByIdOperation {

	private final ReactiveCouchbaseTemplate template;
	private static final Logger LOG = LoggerFactory.getLogger(ReactiveExistsByIdOperationSupport.class);

	ReactiveExistsByIdOperationSupport(ReactiveCouchbaseTemplate template) {
		this.template = template;
	}

	@Override
	@Deprecated
	public ReactiveExistsById existsById() {
		return existsById(null);
	}

	@Override
	public ReactiveExistsById existsById(Class<?> domainType) {
		return new ReactiveExistsByIdSupport(template, domainType, null, null, null);
	}

	static class ReactiveExistsByIdSupport implements ReactiveExistsById {

		private final ReactiveCouchbaseTemplate template;
		private final Class<?> domainType;
		private final String scope;
		private final String collection;
		private final ExistsOptions options;

		ReactiveExistsByIdSupport(final ReactiveCouchbaseTemplate template, final Class<?> domainType, final String scope,
				final String collection, final ExistsOptions options) {
			this.template = template;
			this.domainType = domainType;
			this.scope = scope;
			this.collection = collection;
			this.options = options;
		}

		@Override
		public Mono<Boolean> one(final String id) {
			PseudoArgs<ExistsOptions> pArgs = new PseudoArgs<>(template, scope, collection, options, null, domainType);
			LOG.trace("existsById {}", pArgs);
			return Mono.just(id)
					.flatMap(docId -> template.getCouchbaseClientFactory().withScope(pArgs.getScope())
							.getCollection(pArgs.getCollection()).block().reactive().exists(id, buildOptions(pArgs.getOptions()))
							.map(ExistsResult::exists))
					.onErrorMap(throwable -> {
						if (throwable instanceof RuntimeException) {
							return template.potentiallyConvertRuntimeException((RuntimeException) throwable);
						} else {
							return throwable;
						}
					});
		}

		private ExistsOptions buildOptions(ExistsOptions options) {
			return OptionsBuilder.buildExistsOptions(options);
		}

		@Override
		public Mono<Map<String, Boolean>> all(final Collection<String> ids) {
			return Flux.fromIterable(ids).flatMap(id -> one(id).map(result -> Tuples.of(id, result)))
					.collectMap(Tuple2::getT1, Tuple2::getT2);
		}

		@Override
		public ExistsByIdWithOptions inCollection(final String collection) {
			return new ReactiveExistsByIdSupport(template, domainType, scope, collection, options);
		}

		@Override
		public ExistsByIdInScope withOptions(final ExistsOptions options) {
			Assert.notNull(options, "Options must not be null.");
			return new ReactiveExistsByIdSupport(template, domainType, scope, collection, options);
		}

		@Override
		public ExistsByIdInCollection inScope(final String scope) {
			return new ReactiveExistsByIdSupport(template, domainType, scope, collection, options);
		}

	}

}
