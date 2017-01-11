/*
 * Copyright 2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.couchbase.repository.query;

import static com.couchbase.client.java.query.Select.select;
import static com.couchbase.client.java.query.dsl.functions.AggregateFunctions.count;

import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonValue;
import com.couchbase.client.java.query.Statement;
import com.couchbase.client.java.query.dsl.Expression;
import com.couchbase.client.java.query.dsl.path.FromPath;
import com.couchbase.client.java.query.dsl.path.LimitPath;
import com.couchbase.client.java.query.dsl.path.WherePath;
import org.springframework.data.couchbase.core.RxJavaCouchbaseOperations;
import org.springframework.data.couchbase.repository.query.support.N1qlUtils;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.ReturnedType;
import org.springframework.data.repository.query.parser.PartTree;

/**
 * A reactive {@link RepositoryQuery} for Couchbase, based on query derivation
 *
 * @author Subhashni Balakrishnan
 * @since 3.0
 */
public class ReactivePartTreeN1qlBasedQuery extends ReactiveAbstractN1qlBasedQuery {

    private final PartTree partTree;

    public ReactivePartTreeN1qlBasedQuery(CouchbaseQueryMethod queryMethod, RxJavaCouchbaseOperations operations) {
        super(queryMethod, operations);
        this.partTree = new PartTree(queryMethod.getName(), queryMethod.getEntityInformation().getJavaType());
    }

    @Override
    protected JsonValue getPlaceholderValues(ParameterAccessor accessor) {
        return JsonArray.empty();
    }

    @Override
    protected Statement getStatement(ParameterAccessor accessor, Object[] runtimeParameters, ReturnedType returnedType) {
        String bucketName = getCouchbaseOperations().getCouchbaseBucket().name();
        Expression bucket = N1qlUtils.escapedBucket(bucketName);

        FromPath select;
        if (partTree.isCountProjection()) {
            select = select(count("*"));
        } else {
            select = N1qlUtils.createSelectClauseForEntity(bucketName, returnedType, this.getCouchbaseOperations().getConverter());
        }
        WherePath selectFrom = select.from(bucket);

        N1qlQueryCreator queryCreator = new N1qlQueryCreator(partTree, accessor, selectFrom,
                getCouchbaseOperations().getConverter(), getQueryMethod());
        LimitPath selectFromWhereOrderBy = queryCreator.createQuery();
        if (partTree.isLimiting()) {
            return selectFromWhereOrderBy.limit(partTree.getMaxResults());
        } else {
            return selectFromWhereOrderBy;
        }
    }
}
