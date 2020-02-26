package org.springframework.data.couchbase.domain;

import org.springframework.data.couchbase.repository.ScanConsistency;
import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.stereotype.Repository;

import com.couchbase.client.java.query.QueryScanConsistency;

@Repository
public interface AirportRepository extends PagingAndSortingRepository<Airport, String> {

	@Override
	@ScanConsistency(query = QueryScanConsistency.REQUEST_PLUS)
	Iterable<Airport> findAll();

}
