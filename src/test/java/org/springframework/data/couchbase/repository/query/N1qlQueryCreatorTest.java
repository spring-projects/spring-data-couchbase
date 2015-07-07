package org.springframework.data.couchbase.repository.query;

import static org.junit.Assert.assertEquals;
import static org.springframework.data.couchbase.repository.query.N1qlQueryCreator.createExpression;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import com.couchbase.client.java.query.dsl.Expression;
import org.junit.Test;

import org.springframework.data.repository.query.parser.Part;

public class N1qlQueryCreatorTest {

  //==== The tests below check mapping between a Part.Type and the corresponding N1QL expression ====

  @Test
  public void testBETWEEN() throws Exception {
    Part.Type keyword = Part.Type.BETWEEN;
    Iterator<Object> values = Arrays.<Object>asList("a", "b", 1, 2, "c", "d").iterator();
    String expected = "doc.field BETWEEN \"a\" AND \"b\"";
    String expectedNum = "doc.field BETWEEN 1 AND 2";
    String expectedIgnoreCase = "LOWER(doc.field) BETWEEN LOWER(\"c\") AND LOWER(\"d\")";

    Expression exp = createExpression(keyword, "doc.field", false, values);
    Expression expNum = createExpression(keyword, "doc.field", false, values);
    Expression expIgnoreCase = createExpression(keyword, "doc.field", true, values);

    assertEquals(expected, exp.toString());
    assertEquals(expectedNum, expNum.toString());
    assertEquals(expectedIgnoreCase, expIgnoreCase.toString());
  }

  @Test
  public void testIS_NOT_NULL() throws Exception {
    Part.Type keyword = Part.Type.IS_NOT_NULL;
    Iterator<Object> values = Arrays.<Object>asList("a", 1, "b").iterator();
    String expected = "doc.field IS NOT NULL";
    String expectedNum = "doc.field IS NOT NULL";
    String expectedIgnoreCase = "LOWER(doc.field) IS NOT NULL";

    Expression exp = createExpression(keyword, "doc.field", false, values);
    Expression expNum = createExpression(keyword, "doc.field", false, values);
    Expression expIgnoreCase = createExpression(keyword, "doc.field", true, values);

    assertEquals(expected, exp.toString());
    assertEquals(expectedNum, expNum.toString());
    assertEquals(expectedIgnoreCase, expIgnoreCase.toString());
  }

  @Test
  public void testIS_NULL() throws Exception {
    Part.Type keyword = Part.Type.IS_NULL;
    Iterator<Object> values = Arrays.<Object>asList("a", 1, "b").iterator();
    String expected = "doc.field IS NULL";
    String expectedNum = "doc.field IS NULL";
    String expectedIgnoreCase = "LOWER(doc.field) IS NULL";

    Expression exp = createExpression(keyword, "doc.field", false, values);
    Expression expNum = createExpression(keyword, "doc.field", false, values);
    Expression expIgnoreCase = createExpression(keyword, "doc.field", true, values);

    assertEquals(expected, exp.toString());
    assertEquals(expectedNum, expNum.toString());
    assertEquals(expectedIgnoreCase, expIgnoreCase.toString());
  }

  @Test
  public void testLESS_THAN() throws Exception {
    Part.Type keyword = Part.Type.LESS_THAN;
    Iterator<Object> values = Arrays.<Object>asList("a", 1, "b").iterator();
    String expected = "doc.field < \"a\"";
    String expectedNum = "doc.field < 1";
    String expectedIgnoreCase = "LOWER(doc.field) < LOWER(\"b\")";

    Expression exp = createExpression(keyword, "doc.field", false, values);
    Expression expNum = createExpression(keyword, "doc.field", false, values);
    Expression expIgnoreCase = createExpression(keyword, "doc.field", true, values);

    assertEquals(expected, exp.toString());
    assertEquals(expectedNum, expNum.toString());
    assertEquals(expectedIgnoreCase, expIgnoreCase.toString());
  }

  @Test
  public void testLESS_THAN_EQUAL() throws Exception {
    Part.Type keyword = Part.Type.LESS_THAN_EQUAL;
    Iterator<Object> values = Arrays.<Object>asList("a", 1, "b").iterator();
    String expected = "doc.field <= \"a\"";
    String expectedNum = "doc.field <= 1";
    String expectedIgnoreCase = "LOWER(doc.field) <= LOWER(\"b\")";

    Expression exp = createExpression(keyword, "doc.field", false, values);
    Expression expNum = createExpression(keyword, "doc.field", false, values);
    Expression expIgnoreCase = createExpression(keyword, "doc.field", true, values);

    assertEquals(expected, exp.toString());
    assertEquals(expectedNum, expNum.toString());
    assertEquals(expectedIgnoreCase, expIgnoreCase.toString());
  }

  @Test
  public void testGREATER_THAN() throws Exception {
    Part.Type keyword = Part.Type.GREATER_THAN;
    Iterator<Object> values = Arrays.<Object>asList("a", 1, "b").iterator();
    String expected = "doc.field > \"a\"";
    String expectedNum = "doc.field > 1";
    String expectedIgnoreCase = "LOWER(doc.field) > LOWER(\"b\")";

    Expression exp = createExpression(keyword, "doc.field", false, values);
    Expression expNum = createExpression(keyword, "doc.field", false, values);
    Expression expIgnoreCase = createExpression(keyword, "doc.field", true, values);

    assertEquals(expected, exp.toString());
    assertEquals(expectedNum, expNum.toString());
    assertEquals(expectedIgnoreCase, expIgnoreCase.toString());
  }

  @Test
  public void testGREATER_THAN_EQUAL() throws Exception {
    Part.Type keyword = Part.Type.GREATER_THAN_EQUAL;
    Iterator<Object> values = Arrays.<Object>asList("a", 1, "b").iterator();
    String expected = "doc.field >= \"a\"";
    String expectedNum = "doc.field >= 1";
    String expectedIgnoreCase = "LOWER(doc.field) >= LOWER(\"b\")";

    Expression exp = createExpression(keyword, "doc.field", false, values);
    Expression expNum = createExpression(keyword, "doc.field", false, values);
    Expression expIgnoreCase = createExpression(keyword, "doc.field", true, values);

    assertEquals(expected, exp.toString());
    assertEquals(expectedNum, expNum.toString());
    assertEquals(expectedIgnoreCase, expIgnoreCase.toString());
  }

  @Test
  public void testBEFORE() throws Exception {
    Part.Type keyword = Part.Type.BEFORE;
    Iterator<Object> values = Arrays.<Object>asList("a", 1, "b").iterator();
    String expected = "doc.field < \"a\"";
    String expectedNum = "doc.field < 1";
    String expectedIgnoreCase = "LOWER(doc.field) < LOWER(\"b\")";

    Expression exp = createExpression(keyword, "doc.field", false, values);
    Expression expNum = createExpression(keyword, "doc.field", false, values);
    Expression expIgnoreCase = createExpression(keyword, "doc.field", true, values);

    assertEquals(expected, exp.toString());
    assertEquals(expectedNum, expNum.toString());
    assertEquals(expectedIgnoreCase, expIgnoreCase.toString());
  }

  @Test
  public void testAFTER() throws Exception {
    Part.Type keyword = Part.Type.AFTER;
    Iterator<Object> values = Arrays.<Object>asList("a", 1, "b").iterator();
    String expected = "doc.field > \"a\"";
    String expectedNum = "doc.field > 1";
    String expectedIgnoreCase = "LOWER(doc.field) > LOWER(\"b\")";

    Expression exp = createExpression(keyword, "doc.field", false, values);
    Expression expNum = createExpression(keyword, "doc.field", false, values);
    Expression expIgnoreCase = createExpression(keyword, "doc.field", true, values);

    assertEquals(expected, exp.toString());
    assertEquals(expectedNum, expNum.toString());
    assertEquals(expectedIgnoreCase, expIgnoreCase.toString());
  }

  @Test
  public void testNOT_LIKE() throws Exception {
    Part.Type keyword = Part.Type.NOT_LIKE;
    Iterator<Object> values = Arrays.<Object>asList("a", 1, "b").iterator();
    String expected = "doc.field NOT LIKE \"a\"";
    String expectedNum = "doc.field NOT LIKE 1";
    String expectedIgnoreCase = "LOWER(doc.field) NOT LIKE LOWER(\"b\")";

    Expression exp = createExpression(keyword, "doc.field", false, values);
    Expression expNum = createExpression(keyword, "doc.field", false, values);
    Expression expIgnoreCase = createExpression(keyword, "doc.field", true, values);

    assertEquals(expected, exp.toString());
    assertEquals(expectedNum, expNum.toString());
    assertEquals(expectedIgnoreCase, expIgnoreCase.toString());
  }

  @Test
  public void testLIKE() throws Exception {
    Part.Type keyword = Part.Type.LIKE;
    Iterator<Object> values = Arrays.<Object>asList("a", 1, "b").iterator();
    String expected = "doc.field LIKE \"a\"";
    String expectedNum = "doc.field LIKE 1";
    String expectedIgnoreCase = "LOWER(doc.field) LIKE LOWER(\"b\")";

    Expression exp = createExpression(keyword, "doc.field", false, values);
    Expression expNum = createExpression(keyword, "doc.field", false, values);
    Expression expIgnoreCase = createExpression(keyword, "doc.field", true, values);

    assertEquals(expected, exp.toString());
    assertEquals(expectedNum, expNum.toString());
    assertEquals(expectedIgnoreCase, expIgnoreCase.toString());
  }

  @Test
  public void testSTARTING_WITH() throws Exception {
    Part.Type keyword = Part.Type.STARTING_WITH;
    Iterator<Object> values = Arrays.<Object>asList("a", 1, "b").iterator();
    String expected = "doc.field LIKE \"a%\"";
    String expectedNum = "doc.field LIKE 1";
    String expectedIgnoreCase = "LOWER(doc.field) LIKE LOWER(\"b%\")";

    Expression exp = createExpression(keyword, "doc.field", false, values);
    Expression expNum = createExpression(keyword, "doc.field", false, values);
    Expression expIgnoreCase = createExpression(keyword, "doc.field", true, values);

    assertEquals(expected, exp.toString());
    assertEquals(expectedNum, expNum.toString());
    assertEquals(expectedIgnoreCase, expIgnoreCase.toString());
  }

  @Test
  public void testENDING_WITH() throws Exception {
    Part.Type keyword = Part.Type.ENDING_WITH;
    Iterator<Object> values = Arrays.<Object>asList("a", 1, "b").iterator();
    String expected = "doc.field LIKE \"%a\"";
    String expectedNum = "doc.field LIKE 1";
    String expectedIgnoreCase = "LOWER(doc.field) LIKE LOWER(\"%b\")";

    Expression exp = createExpression(keyword, "doc.field", false, values);
    Expression expNum = createExpression(keyword, "doc.field", false, values);
    Expression expIgnoreCase = createExpression(keyword, "doc.field", true, values);

    assertEquals(expected, exp.toString());
    assertEquals(expectedNum, expNum.toString());
    assertEquals(expectedIgnoreCase, expIgnoreCase.toString());
  }

  @Test
  public void testNOT_CONTAINING() throws Exception {
    Part.Type keyword = Part.Type.NOT_CONTAINING;
    Iterator<Object> values = Arrays.<Object>asList("a", 1, "b").iterator();
    String expected = "doc.field NOT LIKE \"%a%\"";
    String expectedNum = "doc.field NOT LIKE 1";
    String expectedIgnoreCase = "LOWER(doc.field) NOT LIKE LOWER(\"%b%\")";

    Expression exp = createExpression(keyword, "doc.field", false, values);
    Expression expNum = createExpression(keyword, "doc.field", false, values);
    Expression expIgnoreCase = createExpression(keyword, "doc.field", true, values);

    assertEquals(expected, exp.toString());
    assertEquals(expectedNum, expNum.toString());
    assertEquals(expectedIgnoreCase, expIgnoreCase.toString());
  }

  @Test
  public void testCONTAINING() throws Exception {
    Part.Type keyword = Part.Type.CONTAINING;
    Iterator<Object> values = Arrays.<Object>asList("a", 1, "b").iterator();
    String expected = "doc.field LIKE \"%a%\"";
    String expectedNum = "doc.field LIKE 1";
    String expectedIgnoreCase = "LOWER(doc.field) LIKE LOWER(\"%b%\")";

    Expression exp = createExpression(keyword, "doc.field", false, values);
    Expression expNum = createExpression(keyword, "doc.field", false, values);
    Expression expIgnoreCase = createExpression(keyword, "doc.field", true, values);

    assertEquals(expected, exp.toString());
    assertEquals(expectedNum, expNum.toString());
    assertEquals(expectedIgnoreCase, expIgnoreCase.toString());
  }

  @Test
  public void testNOT_IN() throws Exception {
    Part.Type keyword = Part.Type.NOT_IN;
    Iterator<Object> values = Arrays.<Object>asList("a", 1, "b").iterator();
    String expected = "doc.field NOT IN [\"a\"]";
    String expectedNum = "doc.field NOT IN [1]";
    String expectedIgnoreCase = "LOWER(doc.field) NOT IN [\"b\"]";

    Expression exp = createExpression(keyword, "doc.field", false, values);
    Expression expNum = createExpression(keyword, "doc.field", false, values);
    Expression expIgnoreCase = createExpression(keyword, "doc.field", true, values);

    assertEquals(expected, exp.toString());
    assertEquals(expectedNum, expNum.toString());
    assertEquals(expectedIgnoreCase, expIgnoreCase.toString());
  }

  @Test
  public void testNOTINwithCollection() throws Exception {
    Part.Type keyword = Part.Type.NOT_IN;
    List<Object> val1 = Arrays.<Object>asList("av1", "av2");
    List<Object> val2 = Arrays.<Object>asList("bv1", "bv2");
    Iterator<Object> values = Arrays.<Object>asList(val1, val2).iterator();
    String expected = "doc.field NOT IN [\"av1\",\"av2\"]";
    String expectedIgnoreCase = "LOWER(doc.field) NOT IN [\"bv1\",\"bv2\"]";

    Expression exp = createExpression(keyword, "doc.field", false, values);
    Expression expIgnoreCase = createExpression(keyword, "doc.field", true, values);

    assertEquals(expected, exp.toString());
    assertEquals(expectedIgnoreCase, expIgnoreCase.toString());
  }

  @Test
  public void testNOTINwithArray() throws Exception {
    Part.Type keyword = Part.Type.NOT_IN;
    String[] val1 = {"av1", "av2"};
    String[] val2 = {"bv1", "bv2"};
    Iterator<Object> values = Arrays.<Object>asList(val1, val2).iterator();
    String expected = "doc.field NOT IN [\"av1\",\"av2\"]";
    String expectedIgnoreCase = "LOWER(doc.field) NOT IN [\"bv1\",\"bv2\"]";

    Expression exp = createExpression(keyword, "doc.field", false, values);
    Expression expIgnoreCase = createExpression(keyword, "doc.field", true, values);

    assertEquals(expected, exp.toString());
    assertEquals(expectedIgnoreCase, expIgnoreCase.toString());
  }

  @Test
  public void testIN() throws Exception {
    Part.Type keyword = Part.Type.IN;
    Iterator<Object> values = Arrays.<Object>asList("a", 1, "b").iterator();
    String expected = "doc.field IN [\"a\"]";
    String expectedNum = "doc.field IN [1]";
    String expectedIgnoreCase = "LOWER(doc.field) IN [\"b\"]";

    Expression exp = createExpression(keyword, "doc.field", false, values);
    Expression expNum = createExpression(keyword, "doc.field", false, values);
    Expression expIgnoreCase = createExpression(keyword, "doc.field", true, values);

    assertEquals(expected, exp.toString());
    assertEquals(expectedNum, expNum.toString());
    assertEquals(expectedIgnoreCase, expIgnoreCase.toString());
  }

  @Test
  public void testINwithCollection() throws Exception {
    Part.Type keyword = Part.Type.IN;
    List<Object> val1 = Arrays.<Object>asList("av1", "av2");
    List<Object> val2 = Arrays.<Object>asList("bv1", "bv2");
    Iterator<Object> values = Arrays.<Object>asList(val1, val2).iterator();
    String expected = "doc.field IN [\"av1\",\"av2\"]";
    String expectedIgnoreCase = "LOWER(doc.field) IN [\"bv1\",\"bv2\"]";

    Expression exp = createExpression(keyword, "doc.field", false, values);
    Expression expIgnoreCase = createExpression(keyword, "doc.field", true, values);

    assertEquals(expected, exp.toString());
    assertEquals(expectedIgnoreCase, expIgnoreCase.toString());
  }

  @Test
  public void testINwithArray() throws Exception {
    Part.Type keyword = Part.Type.IN;
    String[] val1 = {"av1", "av2"};
    String[] val2 = {"bv1", "bv2"};
    Iterator<Object> values = Arrays.<Object>asList(val1, val2).iterator();
    String expected = "doc.field IN [\"av1\",\"av2\"]";
    String expectedIgnoreCase = "LOWER(doc.field) IN [\"bv1\",\"bv2\"]";

    Expression exp = createExpression(keyword, "doc.field", false, values);
    Expression expIgnoreCase = createExpression(keyword, "doc.field", true, values);

    assertEquals(expected, exp.toString());
    assertEquals(expectedIgnoreCase, expIgnoreCase.toString());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNEAR() throws Exception {
    Part.Type keyword = Part.Type.NEAR;
    Iterator<Object> values = Arrays.<Object>asList("a", 1, "b").iterator();

    Expression exp = createExpression(keyword, "doc.field", true, values);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testWITHIN() throws Exception {
    Part.Type keyword = Part.Type.WITHIN;
    Iterator<Object> values = Arrays.<Object>asList("a", 1, "b").iterator();

    Expression exp = createExpression(keyword, "doc.field", false, values);
  }

  @Test
  public void testREGEX() throws Exception {
    Part.Type keyword = Part.Type.REGEX;
    Iterator<Object> values = Arrays.<Object>asList("a", 1, "b").iterator();
    String expected = "REGEXP_LIKE(doc.field, \"a\")";
    String expectedNum = "REGEXP_LIKE(doc.field, \"1\")";
    String expectedIgnoreCase = "REGEXP_LIKE(doc.field, \"b\")";

    Expression exp = createExpression(keyword, "doc.field", false, values);
    Expression expNum = createExpression(keyword, "doc.field", false, values);
    Expression expIgnoreCase = createExpression(keyword, "doc.field", true, values);

    assertEquals(expected, exp.toString());
    assertEquals(expectedNum, expNum.toString());
    assertEquals(expectedIgnoreCase, expIgnoreCase.toString());
  }

  @Test
  public void testEXISTS() throws Exception {
    Part.Type keyword = Part.Type.EXISTS;
    Iterator<Object> values = Arrays.<Object>asList("a", 1, "b").iterator();
    String expected = "doc.field IS NOT MISSING";
    String expectedNum = "doc.field IS NOT MISSING";
    String expectedIgnoreCase = "LOWER(doc.field) IS NOT MISSING";

    Expression exp = createExpression(keyword, "doc.field", false, values);
    Expression expNum = createExpression(keyword, "doc.field", false, values);
    Expression expIgnoreCase = createExpression(keyword, "doc.field", true, values);

    assertEquals(expected, exp.toString());
    assertEquals(expectedNum, expNum.toString());
    assertEquals(expectedIgnoreCase, expIgnoreCase.toString());
  }

  @Test
  public void testTRUE() throws Exception {
    Part.Type keyword = Part.Type.TRUE;
    Iterator<Object> values = Arrays.<Object>asList("a", 1, "b").iterator();
    String expected = "doc.field = TRUE";
    String expectedNum = "doc.field = TRUE";
    String expectedIgnoreCase = "LOWER(doc.field) = TRUE";

    Expression exp = createExpression(keyword, "doc.field", false, values);
    Expression expNum = createExpression(keyword, "doc.field", false, values);
    Expression expIgnoreCase = createExpression(keyword, "doc.field", true, values);

    assertEquals(expected, exp.toString());
    assertEquals(expectedNum, expNum.toString());
    assertEquals(expectedIgnoreCase, expIgnoreCase.toString());
  }

  @Test
  public void testFALSE() throws Exception {
    Part.Type keyword = Part.Type.FALSE;
    Iterator<Object> values = Arrays.<Object>asList("a", 1, "b").iterator();
    String expected = "doc.field = FALSE";
    String expectedNum = "doc.field = FALSE";
    String expectedIgnoreCase = "LOWER(doc.field) = FALSE";

    Expression exp = createExpression(keyword, "doc.field", false, values);
    Expression expNum = createExpression(keyword, "doc.field", false, values);
    Expression expIgnoreCase = createExpression(keyword, "doc.field", true, values);

    assertEquals(expected, exp.toString());
    assertEquals(expectedNum, expNum.toString());
    assertEquals(expectedIgnoreCase, expIgnoreCase.toString());
  }

  @Test
  public void testNEGATING_SIMPLE_PROPERTY() throws Exception {
    Part.Type keyword = Part.Type.NEGATING_SIMPLE_PROPERTY;
    Iterator<Object> values = Arrays.<Object>asList("a", 1, "b").iterator();
    String expected = "doc.field != \"a\"";
    String expectedNum = "doc.field != 1";
    String expectedIgnoreCase = "LOWER(doc.field) != LOWER(\"b\")";

    Expression exp = createExpression(keyword, "doc.field", false, values);
    Expression expNum = createExpression(keyword, "doc.field", false, values);
    Expression expIgnoreCase = createExpression(keyword, "doc.field", true, values);

    assertEquals(expected, exp.toString());
    assertEquals(expectedNum, expNum.toString());
    assertEquals(expectedIgnoreCase, expIgnoreCase.toString());
  }

  @Test
  public void testSIMPLE_PROPERTY() throws Exception {
    Part.Type keyword = Part.Type.SIMPLE_PROPERTY;
    Iterator<Object> values = Arrays.<Object>asList("a", 1, "b").iterator();
    String expected = "doc.field = \"a\"";
    String expectedNum = "doc.field = 1";
    String expectedIgnoreCase = "LOWER(doc.field) = LOWER(\"b\")";

    Expression exp = createExpression(keyword, "doc.field", false, values);
    Expression expNum = createExpression(keyword, "doc.field", false, values);
    Expression expIgnoreCase = createExpression(keyword, "doc.field", true, values);

    assertEquals(expected, exp.toString());
    assertEquals(expectedNum, expNum.toString());
    assertEquals(expectedIgnoreCase, expIgnoreCase.toString());
  }
}