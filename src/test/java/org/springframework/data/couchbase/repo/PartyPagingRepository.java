package org.springframework.data.couchbase.repo;

import org.springframework.data.couchbase.repository.CouchbasePagingAndSortingRepository;
import org.springframework.data.couchbase.repository.Party;
import org.springframework.stereotype.Repository;

@Repository
public interface PartyPagingRepository extends CouchbasePagingAndSortingRepository<Party, String> {
}
