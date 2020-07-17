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

import reactor.core.publisher.Flux;

import org.springframework.data.couchbase.repository.ScanConsistency;
import org.springframework.data.repository.reactive.ReactiveSortingRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

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
	Mono<Airport> save(Airport a);

	@ScanConsistency(query = QueryScanConsistency.REQUEST_PLUS)
	Flux<Airport> findAllByIata(String iata);

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
}
