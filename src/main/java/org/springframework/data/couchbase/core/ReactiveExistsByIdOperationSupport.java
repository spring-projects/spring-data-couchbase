/*
 * Copyright 2012-2020 the original author or authors
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

import static com.couchbase.client.java.kv.ExistsOptions.*;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.util.Collection;
import java.util.Map;

import org.springframework.util.Assert;

import com.couchbase.client.java.kv.ExistsResult;

public class ReactiveExistsByIdOperationSupport implements ReactiveExistsByIdOperation {

	private final ReactiveCouchbaseTemplate template;

	ReactiveExistsByIdOperationSupport(ReactiveCouchbaseTemplate template) {
		this.template = template;
	}

	@Override
	public <T,I> ReactiveExistsById<T,I> existsById() {
		return new ReactiveExistsByIdSupport(template, null);
	}

	static class ReactiveExistsByIdSupport<T,I> implements ReactiveExistsById<T,I> {

		private final ReactiveCouchbaseTemplate template;
		private final String collection;

		ReactiveExistsByIdSupport(final ReactiveCouchbaseTemplate template, final String collection) {
			this.template = template;
			this.collection = collection;
		}

		@Override
		public Mono<Boolean> one(final I id) {
			return Mono.just(id).flatMap(
					docId -> template.getCollection(collection).reactive().exists(id.toString(), existsOptions()).map(ExistsResult::exists))
					.onErrorMap(throwable -> {
						if (throwable instanceof RuntimeException) {
							return template.potentiallyConvertRuntimeException((RuntimeException) throwable);
						} else {
							return throwable;
						}
					});
		}

		@Override
		public Mono<Map<I, Boolean>> all(final Collection<I> ids) {
			return Flux.fromIterable(ids).flatMap(id -> one(id).map(result -> Tuples.of(id, result)))
					.collectMap(Tuple2::getT1, Tuple2::getT2);
		}

		@Override
		public TerminatingExistsById inCollection(final String collection) {
			Assert.hasText(collection, "Collection must not be null nor empty.");
			return new ReactiveExistsByIdSupport(template, collection);
		}

	}

}
