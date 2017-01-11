package org.springframework.data.couchbase.repository.query;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import static org.springframework.data.couchbase.repository.query.StringBasedN1qlQueryParser.*;

public class StringN1QlBasedQueryTest {
  private static final SpelExpressionParser SPEL_PARSER = new SpelExpressionParser();
  private static final EvaluationContext SPEL_EVALUATION_CONTEXT = new StandardEvaluationContext();

  private static String spel(String expression) {
    return "#{" + expression + "}";
  }

  @Test
  public void testReplaceAllFullSelectPlaceholder() throws Exception {
    String statement = spel(SPEL_SELECT_FROM_CLAUSE) + " where " + spel(SPEL_SELECT_FROM_CLAUSE);
    String parsed = new StringBasedN1qlQueryParser(statement, null, "B", "_class", String.class)
    .doParse(SPEL_PARSER, SPEL_EVALUATION_CONTEXT, false);

    assertEquals("SELECT META(`B`).id AS _ID, META(`B`).cas AS _CAS, `B`.* FROM `B` where "
        + "SELECT META(`B`).id AS _ID, META(`B`).cas AS _CAS, `B`.* FROM `B`", parsed);
  }

  @Test
  public void testReplaceAllBucketPlaceholder() throws Exception {
    String statement = "SELECT * FROM " + spel(SPEL_BUCKET) + " WHERE " + spel(SPEL_BUCKET) + ".test = 1";
    String parsed = new StringBasedN1qlQueryParser(statement, null, "B", "_class", String.class)
            .doParse(SPEL_PARSER, SPEL_EVALUATION_CONTEXT, false);

    assertEquals("SELECT * FROM `B` WHERE `B`.test = 1", parsed);
  }

  @Test
  public void testReplaceAllEntityPlaceholder() throws Exception {
    String statement = "SELECT " + spel(SPEL_ENTITY) + " FROM a where a.test = 1 and " + spel(SPEL_ENTITY);
    String parsed = new StringBasedN1qlQueryParser(statement, null, "B", "_class", String.class)
            .doParse(SPEL_PARSER, SPEL_EVALUATION_CONTEXT, false);

    assertEquals("SELECT META(`B`).id AS _ID, META(`B`).cas AS _CAS FROM a where a.test = 1 and "
        + "META(`B`).id AS _ID, META(`B`).cas AS _CAS", parsed);
  }

  @Test
  public void testReplaceTypePlaceholder() throws Exception {
    String statement = "SELECT " + spel(SPEL_ENTITY) + " FROM a WHERE a.test = 1 AND " + spel(SPEL_FILTER);
    String parsed = new StringBasedN1qlQueryParser(statement, null, "B", "@class", String.class)
            .doParse(SPEL_PARSER, SPEL_EVALUATION_CONTEXT, false);

    assertEquals("SELECT META(`B`).id AS _ID, META(`B`).cas AS _CAS FROM a WHERE a.test = 1 AND `@class` = "
        + "\"java.lang.String\"", parsed);
  }

  @Test
  public void testReplaceSelectFromPlaceholderWithCountIfCountTrue() {
    String statement = spel(SPEL_SELECT_FROM_CLAUSE) + " WHERE true";
    String parsed = new StringBasedN1qlQueryParser(statement, null, "B", "_class", String.class)
            .doParse(SPEL_PARSER, SPEL_EVALUATION_CONTEXT, true);

    assertEquals("SELECT COUNT(*) AS " + CountFragment.COUNT_ALIAS + " FROM `B` WHERE true", parsed);
  }
}