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

import static com.couchbase.client.core.io.CollectionIdentifier.DEFAULT_COLLECTION;
import static com.couchbase.client.core.io.CollectionIdentifier.DEFAULT_SCOPE;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.springframework.core.annotation.AliasFor;
import org.springframework.data.couchbase.core.RemoveResult;
import org.springframework.data.couchbase.core.mapping.Expiry;
import org.springframework.data.couchbase.repository.Collection;
import org.springframework.data.couchbase.repository.CouchbaseRepository;
import org.springframework.data.couchbase.repository.DynamicProxyable;
import org.springframework.data.couchbase.repository.Options;
import org.springframework.data.couchbase.repository.Query;
import org.springframework.data.couchbase.repository.ScanConsistency;
import org.springframework.data.couchbase.repository.Scope;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.couchbase.client.java.analytics.AnalyticsScanConsistency;
import com.couchbase.client.java.query.QueryScanConsistency;

/**
 * Airport repository for testing <br>
 * The DynamicProxyable interface exposes airportRepository.withScope(scope), withCollection() and withOptions() It's
 * necessary on the repository object itself because the withScope() etc methods need to return an object of type
 * AirportRepository so that one can code... airportRepository = airportRepository.withScope(scopeName) without having
 * to cast the result.
 *
 * @author Michael Nitschinger
 * @author Michael Reiche
 */
@Repository
// @Scope("repositoryScope")
// @ScanConsistency(query = QueryScanConsistency.REQUEST_PLUS)
public interface AirportRepository extends CouchbaseRepository<Airport, String>, DynamicProxyable<AirportRepository> {

	@Override
	@ScanConsistency(query = QueryScanConsistency.REQUEST_PLUS)
	List<Airport> findAll();

	@ScanConsistency(query = QueryScanConsistency.REQUEST_PLUS)
	List<Airport> findAllByIata(String iata);

	@ScanConsistency(query = QueryScanConsistency.REQUEST_PLUS)
	List<AirportMini> getByIata(String iata);

	@ScanConsistency(query = QueryScanConsistency.REQUEST_PLUS)
	@ComposedMetaAnnotation(collection = "_default", timeoutMs = 1000)
	Airport findByIata(String iata);

	@ScanConsistency(query = QueryScanConsistency.REQUEST_PLUS)
	Airport findByIata(Iata iata);

	// NOT_BOUNDED to test ScanConsistency
	// @ScanConsistency(query = QueryScanConsistency.NOT_BOUNDED)
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

	@Query("SELECT 1 FROM `#{#n1ql.bucket}` WHERE anything = 'count(*)'") // looks like count query, but is not
	Long countBad();

	@Query("SELECT count(*) FROM `#{#n1ql.bucket}`")
	Long countGood();

	@ScanConsistency(query = QueryScanConsistency.REQUEST_PLUS)
	Page<Airport> findAllByIataNot(String iata, Pageable pageable);

	@ScanConsistency(query = QueryScanConsistency.REQUEST_PLUS)
	@Query("#{#n1ql.selectEntity} WHERE #{#n1ql.filter} AND iata != $1")
	Page<Airport> getAllByIataNot(String iata, Pageable pageable);

	@ScanConsistency(query = QueryScanConsistency.REQUEST_PLUS)
	@Query("SELECT iata, \"\" as __id, 0 as __cas from #{#n1ql.bucket} WHERE #{#n1ql.filter} order by meta().id")
	List<String> getStrings();

	@ScanConsistency(query = QueryScanConsistency.REQUEST_PLUS)
	@Query("SELECT length(iata), \"\" as __id, 0 as __cas from #{#n1ql.bucket} WHERE #{#n1ql.filter} order by meta().id")
	List<Long> getLongs();

	@ScanConsistency(query = QueryScanConsistency.REQUEST_PLUS)
	@Query("SELECT iata, icao, \"\" as __id, 0 as __cas from #{#n1ql.bucket} WHERE #{#n1ql.filter} order by meta().id")
	List<String[]> getStringArrays();

	@ScanConsistency(query = QueryScanConsistency.REQUEST_PLUS)
	Optional<Airport> findByIdAndIata(String id, String iata);

	@ScanConsistency(query = QueryScanConsistency.REQUEST_PLUS)
	List<Airport> findDistinctIcaoBy();

	@ScanConsistency(query = QueryScanConsistency.REQUEST_PLUS)
	List<Airport> findDistinctIcaoAndIataBy();

	@ScanConsistency(query = QueryScanConsistency.REQUEST_PLUS)
	Long countDistinctIcaoAndIataBy();

	@ScanConsistency(query = QueryScanConsistency.REQUEST_PLUS)
	Long countDistinctIcaoBy();

	@Query("SELECT 1 FROM `#{#n1ql.bucket}` WHERE #{#n1ql.filter} " + " #{#projectIds != null ? 'AND blah IN $1' : ''} "
			+ " #{#planIds != null ? 'AND blahblah IN $2' : ''} " + " #{#active != null ? 'AND false = $3' : ''} ")
	Long countOne();

	@Retention(RetentionPolicy.RUNTIME)
	@Target({ ElementType.METHOD, ElementType.TYPE })
	// @Meta
	@Scope
	@Collection
	@ScanConsistency
	@Expiry
	@Options
	public @interface ComposedMetaAnnotation {

		// @AliasFor(annotation = Meta.class, attribute = "maxExecutionTimeMs")
		// long execTime() default -1;

		@AliasFor(annotation = ScanConsistency.class, attribute = "query")
		QueryScanConsistency query() default QueryScanConsistency.NOT_BOUNDED;

		@AliasFor(annotation = ScanConsistency.class, attribute = "analytics")
		AnalyticsScanConsistency analytics() default AnalyticsScanConsistency.NOT_BOUNDED;

		@AliasFor(annotation = Scope.class, attribute = "value")
		String scope() default DEFAULT_SCOPE;

		@AliasFor(annotation = Collection.class, attribute = "value")
		String collection() default DEFAULT_COLLECTION;

		@AliasFor(annotation = Expiry.class, attribute = "expiry")
		int expiry() default 0;

		@AliasFor(annotation = Expiry.class, attribute = "expiryUnit")
		TimeUnit expiryUnit() default TimeUnit.SECONDS;

		@AliasFor(annotation = Expiry.class, attribute = "expiryExpression")
		String expiryExpression() default "";

		@AliasFor(annotation = Options.class, attribute = "timeoutMs")
		long timeoutMs() default 0;

	}
}
