/*
 * Copyright 2017-2021 the original author or authors.
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

import org.springframework.data.couchbase.core.RemoveResult;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;

import org.springframework.data.couchbase.repository.Query;
import org.springframework.data.couchbase.repository.ScanConsistency;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.repository.reactive.ReactiveSortingRepository;
import org.springframework.stereotype.Repository;

import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.query.QueryScanConsistency;

/**
 * template class for Reactive Couchbase operations
 *
 * @author Michael Nitschinger
 * @author Michael Reiche
 */
@Repository
public interface ReactiveAirportRepository extends ReactiveSortingRepository<Airport, String> {

	@Override
	@ScanConsistency(query = QueryScanConsistency.REQUEST_PLUS)
	Flux<Airport> findAll();

	@Override
	@ScanConsistency(query = QueryScanConsistency.REQUEST_PLUS)
	Mono<Void> deleteAll();

	@Override
	Mono<Airport> save(Airport a);

	@ScanConsistency(query = QueryScanConsistency.REQUEST_PLUS)
	Flux<Airport> findAllByIata(String iata);

	@Query("#{#n1ql.selectEntity} WHERE #{#n1ql.filter}")
	Flux<Airport> findAllPoliciesByApplicableTypes(String state, JsonArray applicableTypes);

	@Query("#{#n1ql.selectEntity} WHERE #{#n1ql.filter} and icao != $1 ORDER BY effectiveDateTime DESC LIMIT 1")
	Mono<Airport> findPolicySnapshotByPolicyIdAndEffectiveDateTime(String policyId, long effectiveDateTime);

	@Query("#{#n1ql.selectEntity} WHERE #{#n1ql.filter} ORDER BY effectiveDateTime DESC")
	Flux<Airport> findPolicySnapshotAll();

	@Query("#{#n1ql.selectEntity} where iata = $1")
	@ScanConsistency(query = QueryScanConsistency.REQUEST_PLUS)
	Flux<Airport> getAllByIata(String iata);

	@ScanConsistency(query = QueryScanConsistency.REQUEST_PLUS)
	Mono<Long> countByIataIn(String... iatas);

	@ScanConsistency(query = QueryScanConsistency.REQUEST_PLUS)
	Mono<Long> countByIcaoAndIataIn(String icao, String... iatas);

	@Override
	@ScanConsistency(query = QueryScanConsistency.REQUEST_PLUS)
	Mono<Long> count();

	@Override
	@ScanConsistency(query = QueryScanConsistency.REQUEST_PLUS)
	Mono<Airport> findById(String var1);

	// use parameter type PageRequest instead of Pageable. Pageable requires a return type of Page<>
	@ScanConsistency(query = QueryScanConsistency.REQUEST_PLUS)
	Flux<Airport> findAllByIataLike(String iata, final PageRequest page);

	// use parameter type PageRequest instead of Pageable. Pageable requires a return type of Page<>
	@ScanConsistency(query = QueryScanConsistency.REQUEST_PLUS)
	Flux<Airport> findAllByIataLike(String iata);

	@ScanConsistency(query = QueryScanConsistency.REQUEST_PLUS)
	Mono<Airport> findByIata(String iata);

	// This is not efficient. See findAllByIataLike for efficient reactive paging
	default public Mono<Page<Airport>> findAllAirportsPaged(Pageable pageable) {
		return count().flatMap(airportCount -> {
			return findAll(pageable.getSort())
					.buffer(pageable.getPageSize(), (pageable.getPageNumber() * pageable.getPageSize()))
					.elementAt(pageable.getPageNumber(), new ArrayList<>())
					.map(airports -> new PageImpl<Airport>(airports, pageable, airportCount));
		});
	}
}
