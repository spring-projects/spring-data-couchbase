package org.springframework.data.couchbase.transactions;

import org.springframework.data.couchbase.repository.DynamicProxyable;
import org.springframework.data.repository.CrudRepository;

public interface MyAirlineRepository extends CrudRepository<Airline, String>, DynamicProxyable<MyAirlineRepository> {
}
