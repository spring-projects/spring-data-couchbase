/*
 * Copyright 2020 the original author or authors
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
package org.springframework.data.couchbase.core.support;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * A common interface for those that support one(T), all(Collection<T>)
 *
 * @author Michael Reiche
 * @param <T> - the entity class
 */

public interface OneAndAllReactive<T> {
	Mono<T> one();

	Mono<T> first();

	Flux<? extends T> all();

	Mono<Long> count();

	Mono<Boolean> exists();
}
