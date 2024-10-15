package org.springframework.data.couchbase.domain;

import org.springframework.data.couchbase.repository.Collection;
import org.springframework.data.couchbase.repository.Scope;

@Scope("must set scope name")
@Collection("my_collection")
public interface ReactiveAirportMustScopeRepository extends ReactiveAirportRepository {
}
