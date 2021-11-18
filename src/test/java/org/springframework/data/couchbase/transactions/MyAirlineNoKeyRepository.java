package org.springframework.data.couchbase.transactions;

import org.springframework.data.repository.CrudRepository;

public interface MyAirlineNoKeyRepository extends CrudRepository<AirlineNoKey, String> {
}
