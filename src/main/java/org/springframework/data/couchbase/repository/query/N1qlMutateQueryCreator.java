/*
 * Copyright 2017-present the original author or authors
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

package org.springframework.data.couchbase.repository.query;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.data.couchbase.core.convert.CouchbaseConverter;
import org.springframework.data.couchbase.core.query.N1QLExpression;
import org.springframework.data.couchbase.repository.query.support.N1qlQueryCreatorUtils;
import org.springframework.data.couchbase.repository.query.support.N1qlUtils;
import org.springframework.data.domain.Sort;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.data.repository.query.parser.AbstractQueryCreator;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.data.repository.query.parser.PartTree;

import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonValue;

/**
 * N1qlMutateQueryCreator allows to create queries for delete operations. See {@link OldN1qlQueryCreator} for part types
 * supported
 *
 * @author Subhashni Balakrishnan
 * @author Michael Reiche
 */
public class N1qlMutateQueryCreator extends AbstractQueryCreator<N1QLExpression, N1QLExpression>
		implements PartTreeN1qlQueryCreator {
	private final N1QLExpression mutateFrom;
	private final CouchbaseConverter converter;
	private final CouchbaseQueryMethod queryMethod;
	private final ParameterAccessor accessor;
	private final JsonArray placeHolderValues;
	private final AtomicInteger position;

	public N1qlMutateQueryCreator(PartTree tree, ParameterAccessor parameters, N1QLExpression mutateFrom,
			CouchbaseConverter converter, CouchbaseQueryMethod queryMethod) {
		super(tree, parameters);
		this.mutateFrom = mutateFrom;
		this.converter = converter;
		this.queryMethod = queryMethod;
		this.accessor = parameters;
		this.placeHolderValues = JsonArray.create();
		this.position = new AtomicInteger(1);
	}

	@Override
	protected N1QLExpression create(Part part, Iterator<Object> iterator) {
		return N1qlQueryCreatorUtils.prepareExpression(this.converter, part, iterator, this.position,
				this.placeHolderValues);
	}

	@Override
	protected N1QLExpression and(Part part, N1QLExpression base, Iterator<Object> iterator) {
		if (base == null) {
			return create(part, iterator);
		}

		return base.and(create(part, iterator));
	}

	@Override
	protected N1QLExpression or(N1QLExpression base, N1QLExpression criteria) {
		return base.or(criteria);
	}

	@Override
	protected N1QLExpression complete(N1QLExpression criteria, Sort sort) {
		N1QLExpression whereCriteria = N1qlUtils.createWhereFilterForEntity(criteria, this.converter,
				this.queryMethod.getEntityInformation());
        if (whereCriteria == null) {
            return mutateFrom;
        }
		return mutateFrom.where(whereCriteria);
	}

	@Override
	public JsonValue getPlaceHolderValues() {
		return this.placeHolderValues;
	}
}
