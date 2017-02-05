package org.springframework.data.couchbase.repository;

import java.util.Date;

import org.springframework.data.couchbase.core.query.N1qlSecondaryIndexed;
import org.springframework.data.couchbase.core.query.Query;
import org.springframework.data.couchbase.core.query.View;
import org.springframework.data.couchbase.core.query.ViewIndexed;
import org.springframework.data.repository.query.Param;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * @author Subhashni Balakrishnan
 */
@ViewIndexed(designDoc = "reactiveParty", viewName = "all")
@N1qlSecondaryIndexed(indexName = "reactiveParty")
public interface ReactivePartyRepository extends ReactiveCouchbaseRepository<Party, String> {

	Flux<Party> findByAttendeesGreaterThanEqual(int minAttendees);

	Flux<Party> findByEventDateIs(Date targetDate);

	@View(designDocument = "reactiveParty", viewName = "byDate")
	Flux<Party> findFirst3ByEventDateGreaterThanEqual(Date targetDate);

	Flux<Object> findAllByDescriptionNotNull();

	Mono<Long> countAllByDescriptionNotNull();

	@Query("SELECT MAX(attendees) FROM #{#n1ql.bucket} WHERE #{#n1ql.filter}")
	Mono<Long> findMaxAttendees();

	@Query("SELECT `desc` FROM #{#n1ql.bucket} WHERE #{#n1ql.filter}")
	Mono<String> findSomeString();

	@Query("SELECT count(*) + 5 FROM #{#n1ql.bucket} WHERE #{#n1ql.filter}")
	Mono<Long> countCustomPlusFive();

	@Query("#{#n1ql.selectEntity} WHERE #{#n1ql.filter}")
	Mono<Long> countCustom();

	@Query("SELECT 1 = 1")
	Mono<Boolean> justABoolean();

	@Query("#{#n1ql.selectEntity} WHERE #{#n1ql.filter} AND `desc` LIKE '%' || $included ||  '%' AND attendees >= $min" +
			" AND `desc` NOT LIKE '%' || $excluded || '%'")
	Flux<Party> findAllWithNamedParams(@Param("excluded") String ex, @Param("included") String inc, @Param("min") long minimumAttendees);

	@Query("#{#n1ql.selectEntity} WHERE #{#n1ql.filter} AND `desc` LIKE '%' || $2 ||  '%' AND attendees >= $3" +
			" AND `desc` NOT LIKE '%' || $1 || '%'")
	Flux<Party> findAllWithPositionalParams(String ex, String inc, long minimumAttendees);

	@Query("#{#n1ql.selectEntity} WHERE #{#n1ql.filter} AND `desc` LIKE '%' || $2 ||  '%' AND attendees >= $3" +
			" AND `desc` NOT LIKE '%' || $1 || '%' AND `desc` != \"this is \\\"$excluded\\\"\"")
	Flux<Party> findAllWithPositionalParamsAndQuotedNamedParams(@Param("excluded") String ex, @Param("included") String inc, @Param("min") long min);

	Flux<Party> findByDescriptionOrName(String description, String name);

}
