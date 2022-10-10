/*
 * Copyright 2017-2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.couchbase.repository.query;

import static org.springframework.data.couchbase.core.query.N1QLExpression.count;
import static org.springframework.data.couchbase.core.query.N1QLExpression.select;
import static org.springframework.data.couchbase.core.query.N1QLExpression.x;

import org.springframework.data.couchbase.core.ReactiveCouchbaseOperations;
import org.springframework.data.couchbase.core.query.N1QLExpression;
import org.springframework.data.couchbase.repository.query.support.N1qlUtils;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.ReturnedType;
import org.springframework.data.repository.query.parser.PartTree;

import com.couchbase.client.java.json.JsonValue;

/**
 * A reactive {@link RepositoryQuery} for Couchbase, based on query derivation
 *
 * @author Subhashni Balakrishnan
 * @author Michael Reiche
 * @since 3.0
 * @deprecated
 */
@Deprecated
public class ReactivePartTreeN1qlBasedQuery extends ReactiveAbstractN1qlBasedQuery {

	private final PartTree partTree;
	private JsonValue placeHolderValues;

	public ReactivePartTreeN1qlBasedQuery(CouchbaseQueryMethod queryMethod, ReactiveCouchbaseOperations operations) {
		super(queryMethod, operations);
		this.partTree = new PartTree(queryMethod.getName(), queryMethod.getEntityInformation().getJavaType());
		throw new RuntimeException("deprecated");
	}

	@Override
	protected JsonValue getPlaceholderValues(ParameterAccessor accessor) {
		return this.placeHolderValues;
	}

	@Override
	protected N1QLExpression getExpression(ParameterAccessor accessor, Object[] runtimeParameters,
			ReturnedType returnedType) {
		String bucketName = getCouchbaseOperations().getBucketName();
		N1QLExpression bucket = N1qlUtils.escapedBucket(bucketName);

		N1QLExpression select;
		if (partTree.isCountProjection()) {
			select = select(count(x("*")));
		} else {
			select = N1qlUtils.createSelectClauseForEntity(bucketName, returnedType,
					this.getCouchbaseOperations().getConverter());
		}
		N1QLExpression selectFrom = select.from(bucket);

		OldN1qlQueryCreator queryCreator = new OldN1qlQueryCreator(partTree, accessor, selectFrom,
				getCouchbaseOperations().getConverter(), getQueryMethod());
		N1QLExpression selectFromWhereOrderBy = queryCreator.createQuery();
		this.placeHolderValues = queryCreator.getPlaceHolderValues();
		if (partTree.isLimiting()) {
			return selectFromWhereOrderBy.limit(partTree.getMaxResults());
		} else {
			return selectFromWhereOrderBy;
		}
	}
}
