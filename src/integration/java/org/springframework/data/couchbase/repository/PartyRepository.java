package org.springframework.data.couchbase.repository;

import java.util.Date;
import java.util.List;

import org.springframework.data.couchbase.core.query.N1qlSecondaryIndexed;
import org.springframework.data.couchbase.core.query.Query;
import org.springframework.data.couchbase.core.query.View;
import org.springframework.data.couchbase.core.query.ViewIndexed;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.repository.query.Param;

/**
 * @author Simon Baslé
 */
@ViewIndexed(designDoc = "party", viewName = "all")
@N1qlSecondaryIndexed(indexName = "party")
public interface PartyRepository extends CouchbaseRepository<Party, String> {

  List<Party> findByAttendeesGreaterThanEqual(int minAttendees);

  List<Party> findByEventDateIs(Date targetDate);

  @View(designDocument = "party", viewName = "byDate")
  List<Party> findFirst3ByEventDateGreaterThanEqual(Date targetDate);

  List<Object> findAllByDescriptionNotNull();

  long countAllByDescriptionNotNull();

  @Query("SELECT MAX(attendees) FROM #{#n1ql.bucket} WHERE #{#n1ql.filter}")
  long findMaxAttendees();

  @Query("SELECT `desc` FROM #{#n1ql.bucket} WHERE #{#n1ql.filter}")
  String findSomeString();

  @Query("SELECT count(*) + 5 FROM #{#n1ql.bucket} WHERE #{#n1ql.filter}")
  long countCustomPlusFive();

  @Query("#{#n1ql.selectEntity} WHERE #{#n1ql.filter}")
  long countCustom();

  @Query("SELECT 1 = 1")
  boolean justABoolean();

  @Query("#{#n1ql.selectEntity} WHERE #{#n1ql.filter} AND `attendees` >= $1")
  Page<Party> findPartiesWithAttendee(int count, Pageable pageable);

  @Query("#{#n1ql.selectEntity} WHERE #{#n1ql.filter} AND `desc` LIKE '%' || $included ||  '%' AND attendees >= $min" +
      " AND `desc` NOT LIKE '%' || $excluded || '%'")
  List<Party> findAllWithNamedParams(@Param("excluded") String ex, @Param("included") String inc, @Param("min") long minimumAttendees);

  @Query("#{#n1ql.selectEntity} WHERE #{#n1ql.filter} AND `desc` LIKE '%' || $2 ||  '%' AND attendees >= $3" +
      " AND `desc` NOT LIKE '%' || $1 || '%'")
  List<Party> findAllWithPositionalParams(String ex, String inc, long minimumAttendees);

  @Query("#{#n1ql.selectEntity} WHERE #{#n1ql.filter} AND `desc` LIKE '%' || $2 ||  '%' AND attendees >= $3" +
      " AND `desc` NOT LIKE '%' || $1 || '%' AND `desc` != \"this is \\\"$excluded\\\"\"")
  List<Party> findAllWithPositionalParamsAndQuotedNamedParams(@Param("excluded") String ex, @Param("included") String inc, @Param("min") long min);

  List<Party> findByDescriptionOrName(String description, String name);

}
