package org.springframework.data.couchbase.repository;

import java.util.Date;
import java.util.List;

import org.springframework.data.couchbase.core.query.Query;
import org.springframework.data.couchbase.core.query.View;
import org.springframework.data.couchbase.core.query.ViewIndexed;

/**
 * @author Simon Baslé
 */
@ViewIndexed(designDoc = "party", viewName = "all")
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
}
