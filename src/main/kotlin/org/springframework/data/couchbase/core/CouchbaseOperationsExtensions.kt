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

import com.couchbase.client.java.query.N1qlQuery
import com.couchbase.client.java.view.SpatialViewQuery
import com.couchbase.client.java.view.ViewQuery

/**
 * Kotlin extensions for [CouchbaseOperations]
 *
 * @author Subhashni Balakrishnan
 */

/**
 * Extension for [CouchbaseOperations.findById] leveraging reified type
 * @param id the identifier of the document.
 * @return the document fetched from couchbase.
 */
inline fun <reified T: Any> CouchbaseOperations.findById(id: String) : T =
    findById(id, T::class.java)

/**
 * Extension for [CouchbaseOperations.findByView] leveraging reified type.
 * @param query a [ViewQuery] instance that defines the query.
 * @return list of entities satisfying the view query.
 */
inline fun <reified T: Any> CouchbaseOperations.findByView(query: ViewQuery) : List<T> =
    findByView(query, T::class.java)

/**
 * Extension for [CouchbaseOperations.findBySpatialView] leveraging reified type.
 * @param query a [SpatialViewQuery] instance that defines the query.
 * @return list of entities satisfying the spatial view query.
 */
inline fun <reified T: Any> CouchbaseOperations.findBySpatialView(query: SpatialViewQuery) : List<T> =
    findBySpatialView(query, T::class.java)

/**
 * Extension for [CouchbaseOperations.findByN1QL] leveraging reified type.
 * @param query a [N1qlQuery] instance that defines the query.
 * @return list of entities satisfying the N1ql query.
 */
inline fun <reified T: Any> CouchbaseOperations.findByN1QL(query: N1qlQuery) : List<T> =
    findByN1QL(query, T::class.java)

/**
 * Extension for [CouchbaseOperations.findByN1QLProjection] leveraging reified type.
 * @param query a [N1qlQuery] instance that defines the query.
 * @return list of entities satisfying the N1ql query projection.
 */
inline fun <reified T: Any> CouchbaseOperations.findByN1QLProjection(query: N1qlQuery) : List<T> =
    findByN1QLProjection(query, T::class.java)
