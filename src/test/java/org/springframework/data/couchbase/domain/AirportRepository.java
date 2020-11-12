/*
 * Copyright 2017-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.couchbase.domain;

import java.util.List;

import org.springframework.data.couchbase.repository.Query;
import org.springframework.data.couchbase.repository.ScanConsistency;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.couchbase.client.java.query.QueryScanConsistency;

/**
 * template class for Reactive Couchbase operations
 *
 * @author Michael Nitschinger
 * @author Michael Reiche
 */
@Repository
public interface AirportRepository extends PagingAndSortingRepository<Airport, String> {

	@Override
	@ScanConsistency(query = QueryScanConsistency.REQUEST_PLUS)
	Iterable<Airport> findAll();

	@Override
	Airport save(Airport airport);

	@ScanConsistency(query = QueryScanConsistency.REQUEST_PLUS)
	List<Airport> findAllByIata(String iata);

	@ScanConsistency(query = QueryScanConsistency.REQUEST_PLUS)
	Airport findByIata(String iata);

	@Query("#{#n1ql.selectEntity} where iata = $1")
	@ScanConsistency(query = QueryScanConsistency.REQUEST_PLUS)
	List<Airport> getAllByIata(String iata);

	@ScanConsistency(query = QueryScanConsistency.REQUEST_PLUS)
	long countByIataIn(String... iata);

	@ScanConsistency(query = QueryScanConsistency.REQUEST_PLUS)
	long countByIcaoAndIataIn(String icao, String... iata);

	@ScanConsistency(query = QueryScanConsistency.REQUEST_PLUS)
	long countByIcaoOrIataIn(String icao, String... iata);

	@Override
	@ScanConsistency(query = QueryScanConsistency.REQUEST_PLUS)
	long count();

	@Query("#{#n1ql.selectEntity} WHERE #{#n1ql.filter}  #{#projectIds != null ? 'AND iata IN $1' : ''} "
			+ " #{#planIds != null ? 'AND icao IN $2' : ''}  #{#active != null ? 'AND false = $3' : ''} ")
	@ScanConsistency(query = QueryScanConsistency.REQUEST_PLUS)
	Long countFancyExpression(@Param("projectIds") List<String> projectIds, @Param("planIds") List<String> planIds,
			@Param("active") Boolean active);

	@ScanConsistency(query = QueryScanConsistency.REQUEST_PLUS)
	Page<Airport> findAllByIataNot(String iata, Pageable pageable);
}
