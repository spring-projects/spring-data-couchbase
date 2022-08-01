/*
 * Copyright 2022 the original author or authors.
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
package com.example.demo;

import com.couchbase.client.java.query.QueryScanConsistency;
import org.springframework.data.couchbase.core.RemoveResult;
import org.springframework.data.couchbase.core.mapping.Document;
import org.springframework.data.couchbase.repository.CouchbaseRepository;
import org.springframework.data.couchbase.repository.Query;
import org.springframework.data.couchbase.repository.ScanConsistency;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

/**
 * Airport repository for testing <br>
 * 
 * @author Michael Reiche
 */
@Repository
@Document
public interface AirportRepository extends CouchbaseRepository<Airport, String> {

	// override an annotate with REQUEST_PLUS
	@Override
	@ScanConsistency(query = QueryScanConsistency.REQUEST_PLUS)
	List<Airport> findAll();

	@ScanConsistency(query = QueryScanConsistency.REQUEST_PLUS)
	List<Airport> findAllByIata(String iata);

	@ScanConsistency(query = QueryScanConsistency.REQUEST_PLUS)
	Airport findByIata(String iata);

        @ScanConsistency(query = QueryScanConsistency.NOT_BOUNDED)
	Airport iata(String iata);

	@Query("#{#n1ql.selectEntity} where iata = $1")
	@ScanConsistency(query = QueryScanConsistency.REQUEST_PLUS)
	List<Airport> getAllByIata(String iata);

	@Query("#{#n1ql.delete} WHERE #{#n1ql.filter} and  iata = $1 #{#n1ql.returning}")
	@ScanConsistency(query = QueryScanConsistency.REQUEST_PLUS)
	List<RemoveResult> deleteByIata(String iata);

	@Query("SELECT __cas, * from `#{#n1ql.bucket}` where iata = $1")
	@ScanConsistency(query = QueryScanConsistency.REQUEST_PLUS)
	List<Airport> getAllByIataNoID(String iata);

	@Query("SELECT __id, * from `#{#n1ql.bucket}` where iata = $1")
	@ScanConsistency(query = QueryScanConsistency.REQUEST_PLUS)
	List<Airport> getAllByIataNoCAS(String iata);

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

	@ScanConsistency(query = QueryScanConsistency.REQUEST_PLUS)
	Optional<Airport> findByIdAndIata(String id, String iata);

}
