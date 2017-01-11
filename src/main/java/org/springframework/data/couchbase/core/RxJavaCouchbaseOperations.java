/*
 * Copyright 2017 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.couchbase.core;

import java.util.Collection;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.PersistTo;
import com.couchbase.client.java.ReplicateTo;
import com.couchbase.client.java.cluster.ClusterInfo;
import com.couchbase.client.java.query.AsyncN1qlQueryResult;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.view.AsyncSpatialViewResult;
import com.couchbase.client.java.view.AsyncViewResult;
import com.couchbase.client.java.view.SpatialViewQuery;
import com.couchbase.client.java.view.ViewQuery;
import org.springframework.data.couchbase.core.convert.CouchbaseConverter;
import org.springframework.data.couchbase.core.query.Consistency;
import rx.Observable;

/**
 * @author Subhashni Balakrishnan
 * @since 3.0
 */
public interface RxJavaCouchbaseOperations {

    <T>Observable<T> save(T objectToSave);

    <T>Observable<T> save(Iterable<T> batchToSave);

    <T>Observable<T> save(T objectToSave, PersistTo persistTo, ReplicateTo replicateTo);

    <T>Observable<T> save(Iterable<T> batchToSave, PersistTo persistTo, ReplicateTo replicateTo);

    <T>Observable<T> remove(T objectToRemove);

    <T>Observable<T> remove(T objectToRemove, PersistTo persistTo, ReplicateTo replicateTo);

    <T>Observable<T> remove(Iterable<T> batchToRemove);

    <T>Observable<T> remove(Iterable<T> batchToRemove, PersistTo persistTo, ReplicateTo replicateTo);

    Observable<Boolean> exists(String id);

    <T>Observable<T> findById(String id, Class<T> entityClass);

    Observable<AsyncN1qlQueryResult> queryN1QL(N1qlQuery n1ql);

    Observable<AsyncViewResult> queryView(ViewQuery query);

    Observable<AsyncSpatialViewResult> querySpatialView(SpatialViewQuery query);

    <T>Observable<T> findByView(ViewQuery query, Class<T> entityClass);

    <T>Observable<T> findByN1QL(N1qlQuery n1ql, Class<T> entityClass);

    <T>Observable<T> findBySpatialView(SpatialViewQuery query, Class<T> entityClass);

    <T>Observable<T> findByN1QLProjection(N1qlQuery n1ql, Class<T> fragmentClass);

    Consistency getDefaultConsistency();

    /**
     * Returns the linked {@link Bucket} to this template.
     *
     * @return the client used for the template.
     */
    Bucket getCouchbaseBucket();

    CouchbaseConverter getConverter();

    ClusterInfo getCouchbaseClusterInfo();

}
