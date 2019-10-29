/*
 * Copyright 2018-2019 the original author or authors
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

package org.springframework.data.couchbase.core

import org.springframework.data.couchbase.core.query.N1QLQuery
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

/**
 * Kotlin extensions for [ReactiveJavaCouchbaseOperations]
 *
 * @author Subhashni Balakrishnan
 */

/**
 * Extension for [ReactiveJavaCouchbaseOperations.findById] leveraging reified type
 * @param id the identifier of the document.
 * @return the document fetched from couchbase.
 */
inline fun <reified T: Any> ReactiveJavaCouchbaseOperations.findById(id: String) : Mono<T> =
        findById(id, T::class.java)

/**
 * Extension for [ReactiveJavaCouchbaseOperations.findByN1QL] leveraging reified type.
 * @param query a [N1qlQuery] instance that defines the query.
 * @return list of entities satisfying the n1ql query.
 */
inline fun <reified T: Any> ReactiveJavaCouchbaseOperations.findByN1QL(query: N1QLQuery) : Flux<T> =
        findByN1QL(query, T::class.java)

/**
 * Extension for [ReactiveJavaCouchbaseOperations.findByN1QLProjection] leveraging reified type.
 * @param query a [N1qlQuery] instance that defines the query.
 * @return list of entities satisfying the n1ql query projection.
 */
inline fun <reified T: Any> ReactiveJavaCouchbaseOperations.findByN1QLProjection(query: N1QLQuery) : Flux<T> =
        findByN1QLProjection(query, T::class.java)
