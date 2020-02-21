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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collection;

public interface ExecutableFindByIdOperation {

  <T> ExecutableFindById<T> findById(Class<T> domainType);

  interface TerminatingFindById<T> {

    T one(String id);

    Collection<? extends T> all(Collection<String> ids);

    TerminatingReactiveFindById<T> reactive();

  }

  interface TerminatingReactiveFindById<T> {

    Mono<T> one(String id);

    Flux<? extends T> all(Collection<String> ids);

  }

  interface FindByIdWithCollection<T> extends TerminatingFindById<T> {

    TerminatingFindById<T> inCollection(String collection);
  }

  interface FindByIdWithProjection<T> extends FindByIdWithCollection<T> {

    FindByIdWithCollection<T> project(String... fields);

  }

  interface ExecutableFindById<T> extends FindByIdWithProjection<T> {}

}
