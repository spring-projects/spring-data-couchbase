package org.springframework.data.couchbase.repository.query;

import static org.junit.Assert.*;
import static org.springframework.data.couchbase.repository.query.StringN1qlBasedQuery.PLACEHOLDER_BUCKET;
import static org.springframework.data.couchbase.repository.query.StringN1qlBasedQuery.PLACEHOLDER_ENTITY;
import static org.springframework.data.couchbase.repository.query.StringN1qlBasedQuery.PLACEHOLDER_SELECT_FROM;

import com.couchbase.client.java.query.Statement;
import org.junit.Test;

public class StringN1QlBasedQueryTest {

  @Test
  public void testReplaceFullSelectPlaceholderOnce() throws Exception {
    String statement = PLACEHOLDER_SELECT_FROM + " where " + PLACEHOLDER_SELECT_FROM;
    Statement parsed = StringN1qlBasedQuery.prepare(statement, "B");

    assertEquals("SELECT META(`B`).id AS _ID, META(`B`).cas AS _CAS, `B`.* FROM `B` where "
        + PLACEHOLDER_SELECT_FROM, parsed.toString());
  }

  @Test
  public void testReplaceAllBucketPlaceholder() throws Exception {
    String statement = "SELECT * FROM " + PLACEHOLDER_BUCKET + " WHERE " + PLACEHOLDER_BUCKET + ".test = 1";
    Statement parsed = StringN1qlBasedQuery.prepare(statement, "B");

    assertEquals("SELECT * FROM `B` WHERE `B`.test = 1", parsed.toString());
  }

  @Test
  public void testReplaceFirstEntityPlaceholder() throws Exception {
    String statement = "SELECT " + PLACEHOLDER_ENTITY + " FROM b where b.test = 1 and " + PLACEHOLDER_ENTITY;
    Statement parsed = StringN1qlBasedQuery.prepare(statement, "A");

    assertEquals("SELECT META(`A`).id AS _ID, META(`A`).cas AS _CAS FROM b where b.test = 1 and "
        + PLACEHOLDER_ENTITY, parsed.toString());
  }
}