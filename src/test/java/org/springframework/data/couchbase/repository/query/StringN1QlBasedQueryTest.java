package org.springframework.data.couchbase.repository.query;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.eq;
import static org.springframework.data.couchbase.repository.query.StringBasedN1qlQueryParser.*;
import org.junit.Before;
import org.junit.Test;
import org.springframework.data.couchbase.core.convert.CouchbaseConverter;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

public class StringN1QlBasedQueryTest {
  private static final SpelExpressionParser SPEL_PARSER = new SpelExpressionParser();
  private static final EvaluationContext SPEL_EVALUATION_CONTEXT = new StandardEvaluationContext();

  private static String spel(String expression) {
    return "#{" + expression + "}";
  }
  CouchbaseConverter couchbaseConverter;

  @Before
  public void setup() {
    this.couchbaseConverter = mock(CouchbaseConverter.class);
    when(couchbaseConverter.convertForWriteIfNeeded(eq("value"))).thenReturn("value");
  }

  @Test
  public void testReplaceAllFullSelectPlaceholder() throws Exception {
    String statement = spel(SPEL_SELECT_FROM_CLAUSE) + " where " + spel(SPEL_SELECT_FROM_CLAUSE);
    String parsed = new StringBasedN1qlQueryParser(statement, null, "B", this.couchbaseConverter, "_class", String.class)
    .doParse(SPEL_PARSER, SPEL_EVALUATION_CONTEXT, false);

    assertEquals("SELECT META(`B`).id AS _ID, META(`B`).cas AS _CAS, `B`.* FROM `B` where "
        + "SELECT META(`B`).id AS _ID, META(`B`).cas AS _CAS, `B`.* FROM `B`", parsed);
  }

  @Test
  public void testReplaceAllBucketPlaceholder() throws Exception {
    String statement = "SELECT * FROM " + spel(SPEL_BUCKET) + " WHERE " + spel(SPEL_BUCKET) + ".test = 1";
    String parsed = new StringBasedN1qlQueryParser(statement, null, "B", this.couchbaseConverter, "_class", String.class)
            .doParse(SPEL_PARSER, SPEL_EVALUATION_CONTEXT, false);

    assertEquals("SELECT * FROM `B` WHERE `B`.test = 1", parsed);
  }

  @Test
  public void testReplaceAllEntityPlaceholder() throws Exception {
    String statement = "SELECT " + spel(SPEL_ENTITY) + " FROM a where a.test = 1 and " + spel(SPEL_ENTITY);
    String parsed = new StringBasedN1qlQueryParser(statement, null, "B", this.couchbaseConverter, "_class", String.class)
            .doParse(SPEL_PARSER, SPEL_EVALUATION_CONTEXT, false);

    assertEquals("SELECT META(`B`).id AS _ID, META(`B`).cas AS _CAS FROM a where a.test = 1 and "
        + "META(`B`).id AS _ID, META(`B`).cas AS _CAS", parsed);
  }

  @Test
  public void testReplaceTypePlaceholder() throws Exception {
    String statement = "SELECT " + spel(SPEL_ENTITY) + " FROM a WHERE a.test = 1 AND " + spel(SPEL_FILTER);
    String parsed = new StringBasedN1qlQueryParser(statement, null, "B", this.couchbaseConverter, "@class", String.class)
            .doParse(SPEL_PARSER, SPEL_EVALUATION_CONTEXT, false);

    assertEquals("SELECT META(`B`).id AS _ID, META(`B`).cas AS _CAS FROM a WHERE a.test = 1 AND `@class` = "
        + "\"java.lang.String\"", parsed);
  }

  @Test
  public void testReplaceSelectFromPlaceholderWithCountIfCountTrue() {
    String statement = spel(SPEL_SELECT_FROM_CLAUSE) + " WHERE true";
    String parsed = new StringBasedN1qlQueryParser(statement, null, "B", this.couchbaseConverter, "_class", String.class)
            .doParse(SPEL_PARSER, SPEL_EVALUATION_CONTEXT, true);

    assertEquals("SELECT COUNT(*) AS " + CountFragment.COUNT_ALIAS + " FROM `B` WHERE true", parsed);
  }

  @Test
  public void testDeletePlaceholder() throws Exception {
    String statement = spel(SPEL_DELETE) + " WHERE test = 1 AND " + spel(SPEL_FILTER);
    String parsed = new StringBasedN1qlQueryParser(statement, null, "B", this.couchbaseConverter, "_class", String.class)
            .doParse(SPEL_PARSER, SPEL_EVALUATION_CONTEXT, true);

    assertEquals("DELETE FROM `B` WHERE test = 1 AND `_class` = "
            + "\"java.lang.String\"", parsed);
  }

  @Test
  public void testReturningPlaceholder() throws Exception {
    String statement = spel(SPEL_DELETE) + " WHERE test = 1 AND " + spel(SPEL_FILTER) + spel(SPEL_RETURNING) ;
    String parsed = new StringBasedN1qlQueryParser(statement, null, "B", this.couchbaseConverter, "_class", String.class)
            .doParse(SPEL_PARSER, SPEL_EVALUATION_CONTEXT, true);

    assertEquals("DELETE FROM `B` WHERE test = 1 AND `_class` = "
            + "\"java.lang.String\" returning `B`.*, META(`B`).id AS _ID, META(`B`).cas AS _CAS", parsed);
  }

}
