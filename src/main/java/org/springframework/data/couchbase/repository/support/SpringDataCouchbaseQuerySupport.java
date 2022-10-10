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
package org.springframework.data.couchbase.repository.support;

import java.util.List;
import java.util.Map;

import org.springframework.beans.DirectFieldAccessor;
import org.springframework.data.couchbase.core.mapping.CouchbaseDocument;
import org.springframework.data.domain.Sort;

import com.querydsl.core.support.QueryMixin;
import com.querydsl.core.types.OrderSpecifier;
import com.querydsl.couchbase.document.AbstractCouchbaseQueryDSL;
import com.querydsl.couchbase.document.CouchbaseDocumentSerializer;

/**
 * @author Michael Reiche
 */
abstract class SpringDataCouchbaseQuerySupport<Q extends SpringDataCouchbaseQuerySupport<Q>>
		extends AbstractCouchbaseQueryDSL<Q> {

	private final QueryMixin<Q> superQueryMixin;

	// TODO private static final JsonWriterSettings JSON_WRITER_SETTINGS =
	// JsonWriterSettings.builder().outputMode(JsonMode.SHELL)
	// .build();

	private final CouchbaseDocumentSerializer serializer;

	@SuppressWarnings("unchecked")
	SpringDataCouchbaseQuerySupport(CouchbaseDocumentSerializer serializer) {
		super(serializer);
		this.serializer = serializer;
		DirectFieldAccessor fieldAccessor = new DirectFieldAccessor(this);
		this.superQueryMixin = (QueryMixin<Q>) fieldAccessor.getPropertyValue("queryMixin");
	}

	/**
	 * Returns the representation of the query. <br />
	 * The following query
	 *
	 * <pre class="code">
	 *
	 * where(p.lastname.eq("Matthews")).orderBy(p.firstname.asc()).offset(1).limit(5);
	 * </pre>
	 *
	 * results in
	 *
	 * <pre class="code">
	 *
	 * find({"lastname" : "Matthews"}).sort({"firstname" : 1}).skip(1).limit(5)
	 * </pre>
	 *
	 * Note that encoding to {@link String} may fail when using data types that cannot be encoded or DBRef's without an
	 * identifier.
	 *
	 * @return never {@literal null}.
	 */
	@Override
	public String toString() {

		Map<String, String> projection = createProjection(getQueryMixin().getMetadata().getProjection());
		Sort sort = createSort(getQueryMixin().getMetadata().getOrderBy());
		// TODO DocumentCodec codec = new DocumentCodec(ClientSettings.getDefaultCodecRegistry());

		// TODO
		// StringBuilder sb = new StringBuilder("find(" + asDocument().toJson(JSON_WRITER_SETTINGS, codec));
		StringBuilder sb = new StringBuilder("find(" + asDocument().toString());
		// if (projection != null && projection.isEmpty()) {
		// sb.append(", ").append(projection.toJson(JSON_WRITER_SETTINGS, codec));
		// }
		sb.append(")");
		// TODO
		// if (!sort.isEmpty()) {
		// sb.append(".sort(").append(sort.toJson(JSON_WRITER_SETTINGS, codec)).append(")");
		// }
		if (getQueryMixin().getMetadata().getModifiers().getOffset() != null) {
			sb.append(".skip(").append(getQueryMixin().getMetadata().getModifiers().getOffset()).append(")");
		}
		if (getQueryMixin().getMetadata().getModifiers().getLimit() != null) {
			sb.append(".limit(").append(getQueryMixin().getMetadata().getModifiers().getLimit()).append(")");
		}
		return sb.toString();
	}

	/**
	 * Get the where definition as a Document instance
	 *
	 * @return
	 */
	public CouchbaseDocument asDocument() {
		return createQuery(getQueryMixin().getMetadata().getWhere());
	}

	/**
	 * Obtain the json query representation.
	 *
	 * @return never {@literal null}. public String toJson() { return toJson(JSON_WRITER_SETTINGS); }
	 */

	/**
	 * Obtain the json query representation applying given {@link JsonWriterSettings settings}.
	 *
	 * @param settings must not be {@literal null}.
	 * @return never {@literal null}. public String toJson(JsonWriterSettings settings) { return
	 *         asDocument().toJson(settings); }
	 */

	/**
	 * Compute the sort {@link CouchbaseDocument} from the given list of {@link OrderSpecifier order specifiers}.
	 *
	 * @param orderSpecifiers can be {@literal null}.
	 * @return an empty {@link CouchbaseDocument} if predicate is {@literal null}. see
	 *         CouchbaseDocumentSerializer#toSort(List)
	 */
	protected Sort createSort(List<OrderSpecifier<?>> orderSpecifiers) {
		return null; // TODO serializer.toSort(orderSpecifiers);
	}
}
