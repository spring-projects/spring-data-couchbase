package org.springframework.data.couchbase.repository.query;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.data.couchbase.repository.query.StringN1qlBasedQuery.*;

import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.springframework.expression.EvaluationContext;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

public class StringN1QlBasedQueryTest {

  private static final SpelExpressionParser SPEL_PARSER = new SpelExpressionParser();
  private static final EvaluationContext SPEL_EVALUATION_CONTEXT = new StandardEvaluationContext();

  private StringN1qlBasedQuery mockStringUnderscoreClass;
  private StringN1qlBasedQuery mockStringAtClass;

  @Before
  public void initMock() {
    N1qlSpelValues contextStringUnderscoreClass = StringN1qlBasedQuery.createN1qlSpelValues("B", "_class", String.class, false);
    N1qlSpelValues contextCountStringUnderscoreClass = StringN1qlBasedQuery.createN1qlSpelValues("B", "_class", String.class, true);
    N1qlSpelValues contextStringAtClass = StringN1qlBasedQuery.createN1qlSpelValues("B", "@class", String.class, false);
    N1qlSpelValues contextCountStringAtClass = StringN1qlBasedQuery.createN1qlSpelValues("B", "@class", String.class, true);

    mockStringUnderscoreClass = mock(StringN1qlBasedQuery.class);
    when(mockStringUnderscoreClass.parseSpel(anyString(), anyBoolean(), any(Object[].class)))
        .thenAnswer(mockSpelEvaluation(contextStringUnderscoreClass, contextCountStringUnderscoreClass));

    mockStringAtClass = mock(StringN1qlBasedQuery.class);
    when(mockStringAtClass.parseSpel(anyString(),anyBoolean(), any(Object[].class)))
        .thenAnswer(mockSpelEvaluation(contextStringAtClass, contextCountStringAtClass));
  }

  private static Answer<?> mockSpelEvaluation(final N1qlSpelValues spelValues, final N1qlSpelValues countSpelValues) {
    return new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        String statement = (String) invocation.getArguments()[0];
        boolean isCount = (Boolean) invocation.getArguments()[1];
        if (isCount)
          return doParse(statement, SPEL_PARSER, SPEL_EVALUATION_CONTEXT, countSpelValues);
        else
          return doParse(statement, SPEL_PARSER, SPEL_EVALUATION_CONTEXT, spelValues);
      }
    };
  }

  private static String spel(String expression) {
    return "#{" + expression + "}";
  }

  @Test
  public void testReplaceAllFullSelectPlaceholder() throws Exception {
    String statement = spel(SPEL_SELECT_FROM_CLAUSE) + " where " + spel(SPEL_SELECT_FROM_CLAUSE);
    String parsed = mockStringUnderscoreClass.parseSpel(statement, false, new Object[0]);

    assertEquals("SELECT META(`B`).id AS _ID, META(`B`).cas AS _CAS, `B`.* FROM `B` where "
        + "SELECT META(`B`).id AS _ID, META(`B`).cas AS _CAS, `B`.* FROM `B`", parsed);
  }

  @Test
  public void testReplaceAllBucketPlaceholder() throws Exception {
    String statement = "SELECT * FROM " + spel(SPEL_BUCKET) + " WHERE " + spel(SPEL_BUCKET) + ".test = 1";
    String parsed = mockStringUnderscoreClass.parseSpel(statement, false, new Object[0]);

    assertEquals("SELECT * FROM `B` WHERE `B`.test = 1", parsed);
  }

  @Test
  public void testReplaceAllEntityPlaceholder() throws Exception {
    String statement = "SELECT " + spel(SPEL_ENTITY) + " FROM a where a.test = 1 and " + spel(SPEL_ENTITY);
    String parsed = mockStringUnderscoreClass.parseSpel(statement, false, new Object[0]);

    assertEquals("SELECT META(`B`).id AS _ID, META(`B`).cas AS _CAS FROM a where a.test = 1 and "
        + "META(`B`).id AS _ID, META(`B`).cas AS _CAS", parsed);
  }

  @Test
  public void testReplaceTypePlaceholder() throws Exception {
    String statement = "SELECT " + spel(SPEL_ENTITY) + " FROM a WHERE a.test = 1 AND " + spel(SPEL_FILTER);
    String parsed = mockStringAtClass.parseSpel(statement, false, new Object[0]);

    assertEquals("SELECT META(`B`).id AS _ID, META(`B`).cas AS _CAS FROM a WHERE a.test = 1 AND `@class` = "
        + "\"java.lang.String\"", parsed);
  }

  @Test
  public void testReplaceSelectFromPlaceholderWithCountIfCountTrue() {
    String statement = spel(SPEL_SELECT_FROM_CLAUSE) + " WHERE true";
    String parsed = mockStringUnderscoreClass.parseSpel(statement, true, new Object[0]);

    assertEquals("SELECT COUNT(*) AS " + CountFragment.COUNT_ALIAS + " FROM `B` WHERE true", parsed);
  }
}