package org.springframework.data.couchbase.domain;

import com.couchbase.client.java.query.QueryScanConsistency;
import org.springframework.data.couchbase.repository.CouchbaseRepository;
import org.springframework.data.couchbase.repository.Query;
import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface AirportRepository extends PagingAndSortingRepository<Airport, String> {

  @Override
  @Query(scanConsistency = QueryScanConsistency.REQUEST_PLUS)
  Iterable<Airport> findAll();

}