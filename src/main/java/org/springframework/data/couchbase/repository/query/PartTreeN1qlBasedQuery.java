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

package org.springframework.data.couchbase.repository.query;

import static org.springframework.data.couchbase.core.query.N1QLExpression.count;
import static org.springframework.data.couchbase.core.query.N1QLExpression.delete;
import static org.springframework.data.couchbase.core.query.N1QLExpression.i;
import static org.springframework.data.couchbase.core.query.N1QLExpression.select;
import static org.springframework.data.couchbase.core.query.N1QLExpression.x;
import static org.springframework.data.couchbase.repository.query.support.N1qlUtils.createReturningExpressionForDelete;

import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.couchbase.core.query.N1QLExpression;
import org.springframework.data.couchbase.repository.query.support.N1qlUtils;
import org.springframework.data.domain.Pageable;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.ReturnedType;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.util.Assert;

import com.couchbase.client.java.json.JsonValue;

/**
 * A {@link RepositoryQuery} for Couchbase, based on query derivation
 *
 * @author Simon Baslé
 * @author Subhashni Balakrishnan
 * @author Mark Paluch
 * @author Michael Reiche
 */
@Deprecated
public class PartTreeN1qlBasedQuery extends AbstractN1qlBasedQuery {

	private final PartTree partTree;
	private JsonValue placeHolderValues;

	public PartTreeN1qlBasedQuery(CouchbaseQueryMethod queryMethod, CouchbaseOperations couchbaseOperations) {
		super(queryMethod, couchbaseOperations);
		this.partTree = new PartTree(queryMethod.getName(), queryMethod.getEntityInformation().getJavaType());
		throw new RuntimeException("Deprecated");
	}

	@Override
	protected JsonValue getPlaceholderValues(ParameterAccessor accessor) {
		return this.placeHolderValues;
	}

	@Override
	protected N1QLExpression getCount(ParameterAccessor accessor, Object[] runtimeParameters) {
		N1QLExpression bucket = i(getCouchbaseOperations().getBucketName());
		N1QLExpression countFrom = select(count(x("*")).as(x(CountFragment.COUNT_ALIAS))).from(bucket);

		N1qlCountQueryCreator queryCountCreator = new N1qlCountQueryCreator(partTree, accessor, countFrom,
				getCouchbaseOperations().getConverter(), getQueryMethod());
		N1QLExpression statement = queryCountCreator.createQuery();
		this.placeHolderValues = queryCountCreator.getPlaceHolderValues();
		return statement;
	}

	@Override
	protected N1QLExpression getExpression(ParameterAccessor accessor, Object[] runtimeParameters,
			ReturnedType returnedType) {
		String bucketName = getCouchbaseOperations().getBucketName();
		N1QLExpression bucket = N1qlUtils.escapedBucket(bucketName);

		if (partTree.isDelete()) {
			N1QLExpression deleteUsePath = delete().from(bucket);
			N1qlMutateQueryCreator mutateQueryCreator = new N1qlMutateQueryCreator(partTree, accessor, deleteUsePath,
					getCouchbaseOperations().getConverter(), getQueryMethod());
			N1QLExpression mutateFromWhereOrderBy = mutateQueryCreator.createQuery();
			this.placeHolderValues = mutateQueryCreator.getPlaceHolderValues();

			if (partTree.isLimiting()) {
				return mutateFromWhereOrderBy.limit(partTree.getMaxResults());
			} else {
				return mutateFromWhereOrderBy.returning(createReturningExpressionForDelete(bucketName));
			}
		} else {
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

			if (queryMethod.isPageQuery()) {
				Pageable pageable = accessor.getPageable();
				Assert.notNull(pageable, "Pageable must not be null!");
				return selectFromWhereOrderBy.limit(pageable.getPageSize()).offset(Math.toIntExact(pageable.getOffset()));
			} else if (queryMethod.isSliceQuery() && accessor.getPageable().isPaged()) {
				Pageable pageable = accessor.getPageable();
				Assert.notNull(pageable, "Pageable must not be null!");
				return selectFromWhereOrderBy.limit(pageable.getPageSize() + 1).offset(Math.toIntExact(pageable.getOffset()));
			} else if (partTree.isLimiting()) {
				return selectFromWhereOrderBy.limit(partTree.getMaxResults());
			} else {
				return selectFromWhereOrderBy;
			}
		}
	}

	@Override
	protected boolean useGeneratedCountQuery() {
		return false; // generated count query is just for Page/Slice, not projections
	}
}
