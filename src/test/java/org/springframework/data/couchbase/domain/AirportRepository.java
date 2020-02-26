package org.springframework.data.couchbase.domain;

import com.couchbase.client.java.query.QueryScanConsistency;
import org.springframework.data.couchbase.repository.ScanConsistency;
import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface AirportRepository extends PagingAndSortingRepository<Airport, String> {

  @Override
  @ScanConsistency(query = QueryScanConsistency.REQUEST_PLUS)
  Iterable<Airport> findAll();

}