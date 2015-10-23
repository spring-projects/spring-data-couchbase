package org.springframework.data.couchbase.repository;

import java.util.Date;
import java.util.List;

import org.springframework.data.couchbase.core.query.View;

/**
 * @author Simon Baslé
 */
public interface PartyRepository extends CouchbaseRepository<Party, String> {

  List<Party> findByAttendeesGreaterThanEqual(int minAttendees);

  List<Party> findByEventDateIs(Date targetDate);

  @View(designDocument = "party", viewName = "byDate")
  List<Party> findFirst3ByEventDateGreaterThanEqual(Date targetDate);

  List<Object> findAllByDescriptionNotNull();

}
