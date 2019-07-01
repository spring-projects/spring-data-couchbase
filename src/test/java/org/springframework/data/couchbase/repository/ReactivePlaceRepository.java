package org.springframework.data.couchbase.repository;

import org.springframework.data.couchbase.core.query.N1qlPrimaryIndexed;
import org.springframework.data.couchbase.core.query.Query;
import org.springframework.data.couchbase.core.query.ViewIndexed;
import reactor.core.publisher.Flux;

import java.util.List;

/**
 * @author David Kelly
 */
@N1qlPrimaryIndexed
public interface ReactivePlaceRepository extends ReactiveCouchbaseRepository<ReactivePlace, String> {

    @Query("#{#n1ql.selectEntity} WHERE name = $1")
    Flux<ReactivePlace> findByName(String name);

}

