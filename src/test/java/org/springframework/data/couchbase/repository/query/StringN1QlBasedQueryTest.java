package org.springframework.data.couchbase.repository.query;

import static org.junit.Assert.*;
import static org.springframework.data.couchbase.repository.query.StringN1qlBasedQuery.*;

import com.couchbase.client.java.query.Statement;
import org.junit.Test;

public class StringN1QlBasedQueryTest {

  @Test
  public void testReplaceFullSelectPlaceholderOnce() throws Exception {
    String statement = PLACEHOLDER_SELECT_FROM + " where " + PLACEHOLDER_SELECT_FROM;
    Statement parsed = StringN1qlBasedQuery.prepare(statement, "B", "_class", String.class, false);

    assertEquals("SELECT META(`B`).id AS _ID, META(`B`).cas AS _CAS, `B`.* FROM `B` where "
        + PLACEHOLDER_SELECT_FROM, parsed.toString());
  }

  @Test
  public void testReplaceAllBucketPlaceholder() throws Exception {
    String statement = "SELECT * FROM " + PLACEHOLDER_BUCKET + " WHERE " + PLACEHOLDER_BUCKET + ".test = 1";
    Statement parsed = StringN1qlBasedQuery.prepare(statement, "B", "_class", String.class, false);

    assertEquals("SELECT * FROM `B` WHERE `B`.test = 1", parsed.toString());
  }

  @Test
  public void testReplaceFirstEntityPlaceholder() throws Exception {
    String statement = "SELECT " + PLACEHOLDER_ENTITY + " FROM b where b.test = 1 and " + PLACEHOLDER_ENTITY;
    Statement parsed = StringN1qlBasedQuery.prepare(statement, "A", "_class", String.class, false);

    assertEquals("SELECT META(`A`).id AS _ID, META(`A`).cas AS _CAS FROM b where b.test = 1 and "
        + PLACEHOLDER_ENTITY, parsed.toString());
  }

  @Test
  public void testReplaceTypePlaceholder() throws Exception {
    String statement = "SELECT " + PLACEHOLDER_ENTITY + " FROM b WHERE b.test = 1 AND " + PLACEHOLDER_FILTER_TYPE;
    Statement parsed = StringN1qlBasedQuery.prepare(statement, "A", "@class", String.class, false);

    assertEquals("SELECT META(`A`).id AS _ID, META(`A`).cas AS _CAS FROM b WHERE b.test = 1 AND `@class` = "
        + "\"java.lang.String\"", parsed.toString());
  }

  @Test
  public void testReplaceSelectFromPlaceholderWithCountIfCountTrue() {
    String statement = PLACEHOLDER_SELECT_FROM + " WHERE true";
    Statement parsed = StringN1qlBasedQuery.prepare(statement, "A", "_class", String.class, true);

    assertEquals("SELECT COUNT(*) AS " + CountFragment.COUNT_ALIAS + " FROM `A` WHERE true", parsed.toString());
  }
}