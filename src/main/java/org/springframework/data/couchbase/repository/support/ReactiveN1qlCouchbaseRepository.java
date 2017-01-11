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
package org.springframework.data.couchbase.repository.support;

import java.io.Serializable;

import com.couchbase.client.java.query.N1qlParams;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.Statement;
import com.couchbase.client.java.query.consistency.ScanConsistency;
import com.couchbase.client.java.query.dsl.Expression;
import com.couchbase.client.java.query.dsl.path.WherePath;
import org.springframework.data.couchbase.core.RxJavaCouchbaseOperations;
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

    public ReactiveN1qlCouchbaseRepository(CouchbaseEntityInformation<T, String> metadata, RxJavaCouchbaseOperations operations) {
        super(metadata, operations);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Flux<T> findAll(Sort sort) {
        Assert.notNull(sort);

        //prepare elements of the query
        WherePath selectFrom = N1qlUtils.createSelectFromForEntity(getCouchbaseOperations().getCouchbaseBucket().name());
        Expression whereCriteria = N1qlUtils.createWhereFilterForEntity(null, getCouchbaseOperations().getConverter(),
                getEntityInformation());

        //apply the sort
        com.couchbase.client.java.query.dsl.Sort[] orderings = N1qlUtils.createSort(sort, getCouchbaseOperations().getConverter());
        Statement st = selectFrom.where(whereCriteria).orderBy(orderings);

        //fire the query
        ScanConsistency consistency = getCouchbaseOperations().getDefaultConsistency().n1qlConsistency();
        N1qlQuery query = N1qlQuery.simple(st, N1qlParams.build().consistency(consistency));
        return mapFlux(getCouchbaseOperations().findByN1QL(query, getEntityInformation().getJavaType()));
    }


}
