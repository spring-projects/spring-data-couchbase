/*
 * Copyright 2017-2019 the original author or authors.
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
package org.springframework.data.couchbase.repository.support;

import java.io.Serializable;


import com.couchbase.client.java.query.QueryOptions;

import com.couchbase.client.java.query.QueryScanConsistency;
import org.springframework.data.couchbase.core.ReactiveJavaCouchbaseOperations;
import org.springframework.data.couchbase.core.query.N1QLExpression;
import org.springframework.data.couchbase.core.query.N1QLQuery;
import org.springframework.data.couchbase.repository.ReactiveCouchbaseSortingRepository;
import org.springframework.data.couchbase.repository.query.CouchbaseEntityInformation;
import org.springframework.data.couchbase.repository.query.support.N1qlUtils;
import org.springframework.data.domain.Sort;
import org.springframework.util.Assert;
import reactor.core.publisher.Flux;

/**
 * @author Subhashni Balakrishnan
 * @since 3.0
 */
public class ReactiveN1qlCouchbaseRepository<T, ID extends Serializable>
        extends SimpleReactiveCouchbaseRepository<T, ID>
    implements ReactiveCouchbaseSortingRepository<T, ID> {

    public ReactiveN1qlCouchbaseRepository(CouchbaseEntityInformation<T, String> metadata, ReactiveJavaCouchbaseOperations operations) {
        super(metadata, operations);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Flux<T> findAll(Sort sort) {
        Assert.notNull(sort);

        //prepare elements of the query
        N1QLExpression selectFrom = N1qlUtils.createSelectFromForEntity(getCouchbaseOperations().getCouchbaseBucket().name());
        N1QLExpression whereCriteria = N1qlUtils.createWhereFilterForEntity(null, getCouchbaseOperations().getConverter(),
                getEntityInformation());

        //apply the sort
        N1QLExpression[] orderings = N1qlUtils.createSort(sort);
        N1QLExpression st = selectFrom.where(whereCriteria).orderBy(orderings);

        //fire the query
        QueryScanConsistency consistency = getCouchbaseOperations().getDefaultConsistency().n1qlConsistency();
        N1QLQuery query = new N1QLQuery(st, QueryOptions.queryOptions().scanConsistency(consistency));
        return getCouchbaseOperations().findByN1QL(query, getEntityInformation().getJavaType());
    }


}
