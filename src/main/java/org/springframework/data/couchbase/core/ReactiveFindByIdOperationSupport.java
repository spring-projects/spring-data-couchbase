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

import static com.couchbase.client.java.kv.GetOptions.getOptions;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.springframework.util.Assert;

import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.java.codec.RawJsonTranscoder;
import com.couchbase.client.java.kv.GetOptions;

public class ReactiveFindByIdOperationSupport implements ReactiveFindByIdOperation {

	private final ReactiveCouchbaseTemplate template;

	ReactiveFindByIdOperationSupport(ReactiveCouchbaseTemplate template) {
		this.template = template;
	}

	@Override
	public <T> ReactiveFindById<T> findById(Class<T> domainType) {
		return new ReactiveFindByIdSupport<>(template, domainType, null, null);
	}

	static class ReactiveFindByIdSupport<T> implements ReactiveFindById<T> {

		private final ReactiveCouchbaseTemplate template;
		private final Class<T> domainType;
		private final String collection;
		private final List<String> fields;

		ReactiveFindByIdSupport(ReactiveCouchbaseTemplate template, Class<T> domainType, String collection,
				List<String> fields) {
			this.template = template;
			this.domainType = domainType;
			this.collection = collection;
			this.fields = fields;
		}

		@Override
		public Mono<T> one(final String id) {
			return Mono.just(id).flatMap(docId -> {
				GetOptions options = getOptions().transcoder(RawJsonTranscoder.INSTANCE);
				if (fields != null && !fields.isEmpty()) {
					options.project(fields);
				}
				return template.getCollection(collection).reactive().get(docId, options);
			}).map(result -> template.support().decodeEntity(id, result.contentAs(String.class), result.cas(), domainType))
					.onErrorResume(throwable -> {
						if (throwable instanceof RuntimeException) {
							if (throwable instanceof DocumentNotFoundException) {
								return Mono.empty();
							}
						}
						return Mono.error(throwable);
					}).onErrorMap(throwable -> {
						if (throwable instanceof RuntimeException) {
							return template.potentiallyConvertRuntimeException((RuntimeException) throwable);
						} else {
							return throwable;
						}
					});
		}

		@Override
		public Flux<? extends T> all(final Collection<String> ids) {
			return Flux.fromIterable(ids).flatMap(this::one);
		}

		@Override
		public TerminatingFindById<T> inCollection(final String collection) {
			Assert.hasText(collection, "Collection must not be null nor empty.");
			return new ReactiveFindByIdSupport<>(template, domainType, collection, fields);
		}

		@Override
		public FindByIdWithCollection<T> project(String... fields) {
			Assert.notEmpty(fields, "Fields must not be null nor empty.");
			return new ReactiveFindByIdSupport<>(template, domainType, collection, Arrays.asList(fields));
		}
	}

}
