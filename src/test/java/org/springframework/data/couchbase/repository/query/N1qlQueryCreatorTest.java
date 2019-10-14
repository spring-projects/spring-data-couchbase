package org.springframework.data.couchbase.repository.query;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.query.dsl.Expression;

import org.junit.Test;
import org.springframework.data.couchbase.repository.query.support.N1qlQueryCreatorUtils;
import org.springframework.data.repository.query.parser.Part;

import static org.assertj.core.api.Assertions.assertThat;

public class N1qlQueryCreatorTest {

  //==== The tests below check mapping between a Part.Type and the corresponding N1QL expression ====

  @Test
  public void testBETWEEN() throws Exception {
    Part.Type keyword = Part.Type.BETWEEN;
    Iterator<Object> values = Arrays.<Object>asList("a", "b", 1, 2, "C", "D").iterator();
    String expected = "doc.field BETWEEN $0 AND $1";
    String expectedNum = "doc.field BETWEEN $0 AND $1";
    String expectedIgnoreCase = "LOWER(doc.field) BETWEEN LOWER($0) AND LOWER($1)";

    JsonArray phexp = JsonArray.create();
    Expression exp = N1qlQueryCreatorUtils.createExpression(keyword, "doc.field", false, values, new AtomicInteger(), phexp);
    JsonArray phexpNum = JsonArray.create();
    Expression expNum = N1qlQueryCreatorUtils.createExpression(keyword, "doc.field", false, values, new AtomicInteger(), phexpNum);
    JsonArray phexpIgnoreCase = JsonArray.create();
    Expression expIgnoreCase = N1qlQueryCreatorUtils.createExpression(keyword, "doc.field", true, values, new AtomicInteger(), phexpIgnoreCase);

    assertThat(exp.toString()).isEqualTo(expected);
    assertThat(expNum.toString()).isEqualTo(expectedNum);
    assertThat(expIgnoreCase.toString()).isEqualTo(expectedIgnoreCase);
    assertThat(phexp).isEqualTo(JsonArray.create().add("a").add("b"));
    assertThat(phexpNum).isEqualTo(JsonArray.create().add(1).add(2));
    assertThat(phexpIgnoreCase).isEqualTo(JsonArray.create().add("C").add("D"));
  }

  @Test
  public void testIS_NOT_NULL() throws Exception {
    Part.Type keyword = Part.Type.IS_NOT_NULL;
    Iterator<Object> values = Arrays.<Object>asList("a", 1, "b").iterator();
    String expected = "doc.field IS NOT NULL";
    String expectedNum = "doc.field IS NOT NULL";
    String expectedIgnoreCase = "LOWER(doc.field) IS NOT NULL";

    JsonArray phexp = JsonArray.create();
    Expression exp = N1qlQueryCreatorUtils.createExpression(keyword, "doc.field", false, values, new AtomicInteger(), phexp);
    JsonArray phexpNum = JsonArray.create();
    Expression expNum = N1qlQueryCreatorUtils.createExpression(keyword, "doc.field", false, values, new AtomicInteger(), phexpNum);
    JsonArray phexpIgnoreCase = JsonArray.create();
    Expression expIgnoreCase = N1qlQueryCreatorUtils.createExpression(keyword, "doc.field", true, values, new AtomicInteger(), phexpIgnoreCase);

    assertThat(exp.toString()).isEqualTo(expected);
    assertThat(expNum.toString()).isEqualTo(expectedNum);
    assertThat(expIgnoreCase.toString()).isEqualTo(expectedIgnoreCase);
    assertThat(phexp).isEqualTo(JsonArray.create());
    assertThat(phexpNum).isEqualTo(JsonArray.create());
    assertThat(phexpIgnoreCase).isEqualTo(JsonArray.create());
  }

  @Test
  public void testIS_NULL() throws Exception {
    Part.Type keyword = Part.Type.IS_NULL;
    Iterator<Object> values = Arrays.<Object>asList("a", 1, "b").iterator();
    String expected = "doc.field IS NULL";
    String expectedNum = "doc.field IS NULL";
    String expectedIgnoreCase = "LOWER(doc.field) IS NULL";

    JsonArray phexp = JsonArray.create();
    Expression exp = N1qlQueryCreatorUtils.createExpression(keyword, "doc.field", false, values, new AtomicInteger(), phexp);
    JsonArray phexpNum = JsonArray.create();
    Expression expNum = N1qlQueryCreatorUtils.createExpression(keyword, "doc.field", false, values, new AtomicInteger(), phexpNum);
    JsonArray phexpIgnoreCase = JsonArray.create();
    Expression expIgnoreCase = N1qlQueryCreatorUtils.createExpression(keyword, "doc.field", true, values, new AtomicInteger(), phexpIgnoreCase);

    assertThat(exp.toString()).isEqualTo(expected);
    assertThat(expNum.toString()).isEqualTo(expectedNum);
    assertThat(expIgnoreCase.toString()).isEqualTo(expectedIgnoreCase);
    assertThat(phexp).isEqualTo(JsonArray.create());
    assertThat(phexpNum).isEqualTo(JsonArray.create());
    assertThat(phexpIgnoreCase).isEqualTo(JsonArray.create());
  }

  @Test
  public void testLESS_THAN() throws Exception {
    Part.Type keyword = Part.Type.LESS_THAN;
    Iterator<Object> values = Arrays.<Object>asList("a", 1, "b").iterator();
    String expected = "doc.field < $0";
    String expectedNum = "doc.field < $0";
    String expectedIgnoreCase = "LOWER(doc.field) < LOWER($0)";

    JsonArray phexp = JsonArray.create();
    Expression exp = N1qlQueryCreatorUtils.createExpression(keyword, "doc.field", false, values, new AtomicInteger(), phexp);
    JsonArray phexpNum = JsonArray.create();
    Expression expNum = N1qlQueryCreatorUtils.createExpression(keyword, "doc.field", false, values, new AtomicInteger(), phexpNum);
    JsonArray phexpIgnoreCase = JsonArray.create();
    Expression expIgnoreCase = N1qlQueryCreatorUtils.createExpression(keyword, "doc.field", true, values, new AtomicInteger(), phexpIgnoreCase);

    assertThat(exp.toString()).isEqualTo(expected);
    assertThat(expNum.toString()).isEqualTo(expectedNum);
    assertThat(expIgnoreCase.toString()).isEqualTo(expectedIgnoreCase);
    assertThat(phexp).isEqualTo(JsonArray.create().add("a"));
    assertThat(phexpNum).isEqualTo(JsonArray.create().add(1));
    assertThat(phexpIgnoreCase).isEqualTo(JsonArray.create().add("b"));
  }

  @Test
  public void testLESS_THAN_EQUAL() throws Exception {
    Part.Type keyword = Part.Type.LESS_THAN_EQUAL;
    Iterator<Object> values = Arrays.<Object>asList("a", 1, "b").iterator();
    String expected = "doc.field <= $0";
    String expectedNum = "doc.field <= $0";
    String expectedIgnoreCase = "LOWER(doc.field) <= LOWER($0)";

    JsonArray phexp = JsonArray.create();
    Expression exp = N1qlQueryCreatorUtils.createExpression(keyword, "doc.field", false, values, new AtomicInteger(), phexp);
    JsonArray phexpNum = JsonArray.create();
    Expression expNum = N1qlQueryCreatorUtils.createExpression(keyword, "doc.field", false, values, new AtomicInteger(), phexpNum);
    JsonArray phexpIgnoreCase = JsonArray.create();
    Expression expIgnoreCase = N1qlQueryCreatorUtils.createExpression(keyword, "doc.field", true, values, new AtomicInteger(), phexpIgnoreCase);

    assertThat(exp.toString()).isEqualTo(expected);
    assertThat(expNum.toString()).isEqualTo(expectedNum);
    assertThat(expIgnoreCase.toString()).isEqualTo(expectedIgnoreCase);
    assertThat(phexp).isEqualTo(JsonArray.create().add("a"));
    assertThat(phexpNum).isEqualTo(JsonArray.create().add(1));
    assertThat(phexpIgnoreCase).isEqualTo(JsonArray.create().add("b"));
  }

  @Test
  public void testGREATER_THAN() throws Exception {
    Part.Type keyword = Part.Type.GREATER_THAN;
    Iterator<Object> values = Arrays.<Object>asList("a", 1, "b").iterator();
    String expected = "doc.field > $0";
    String expectedNum = "doc.field > $0";
    String expectedIgnoreCase = "LOWER(doc.field) > LOWER($0)";

    JsonArray phexp = JsonArray.create();
    Expression exp = N1qlQueryCreatorUtils.createExpression(keyword, "doc.field", false, values, new AtomicInteger(), phexp);
    JsonArray phexpNum = JsonArray.create();
    Expression expNum = N1qlQueryCreatorUtils.createExpression(keyword, "doc.field", false, values, new AtomicInteger(), phexpNum);
    JsonArray phexpIgnoreCase = JsonArray.create();
    Expression expIgnoreCase = N1qlQueryCreatorUtils.createExpression(keyword, "doc.field", true, values, new AtomicInteger(), phexpIgnoreCase);

    assertThat(exp.toString()).isEqualTo(expected);
    assertThat(expNum.toString()).isEqualTo(expectedNum);
    assertThat(expIgnoreCase.toString()).isEqualTo(expectedIgnoreCase);
    assertThat(phexp).isEqualTo(JsonArray.create().add("a"));
    assertThat(phexpNum).isEqualTo(JsonArray.create().add(1));
    assertThat(phexpIgnoreCase).isEqualTo(JsonArray.create().add("b"));
  }

  @Test
  public void testGREATER_THAN_EQUAL() throws Exception {
    Part.Type keyword = Part.Type.GREATER_THAN_EQUAL;
    Iterator<Object> values = Arrays.<Object>asList("a", 1, "b").iterator();
    String expected = "doc.field >= $0";
    String expectedNum = "doc.field >= $0";
    String expectedIgnoreCase = "LOWER(doc.field) >= LOWER($0)";

    JsonArray phexp = JsonArray.create();
    Expression exp = N1qlQueryCreatorUtils.createExpression(keyword, "doc.field", false, values, new AtomicInteger(), phexp);
    JsonArray phexpNum = JsonArray.create();
    Expression expNum = N1qlQueryCreatorUtils.createExpression(keyword, "doc.field", false, values, new AtomicInteger(), phexpNum);
    JsonArray phexpIgnoreCase = JsonArray.create();
    Expression expIgnoreCase = N1qlQueryCreatorUtils.createExpression(keyword, "doc.field", true, values, new AtomicInteger(), phexpIgnoreCase);

    assertThat(exp.toString()).isEqualTo(expected);
    assertThat(expNum.toString()).isEqualTo(expectedNum);
    assertThat(expIgnoreCase.toString()).isEqualTo(expectedIgnoreCase);
    assertThat(phexp).isEqualTo(JsonArray.create().add("a"));
    assertThat(phexpNum).isEqualTo(JsonArray.create().add(1));
    assertThat(phexpIgnoreCase).isEqualTo(JsonArray.create().add("b"));
  }

  @Test
  public void testBEFORE() throws Exception {
    Part.Type keyword = Part.Type.BEFORE;
    Iterator<Object> values = Arrays.<Object>asList("a", 1, "b").iterator();
    String expected = "doc.field < $0";
    String expectedNum = "doc.field < $0";
    String expectedIgnoreCase = "LOWER(doc.field) < LOWER($0)";

    JsonArray phexp = JsonArray.create();
    Expression exp = N1qlQueryCreatorUtils.createExpression(keyword, "doc.field", false, values, new AtomicInteger(), phexp);
    JsonArray phexpNum = JsonArray.create();
    Expression expNum = N1qlQueryCreatorUtils.createExpression(keyword, "doc.field", false, values, new AtomicInteger(), phexpNum);
    JsonArray phexpIgnoreCase = JsonArray.create();
    Expression expIgnoreCase = N1qlQueryCreatorUtils.createExpression(keyword, "doc.field", true, values, new AtomicInteger(), phexpIgnoreCase);

    assertThat(exp.toString()).isEqualTo(expected);
    assertThat(expNum.toString()).isEqualTo(expectedNum);
    assertThat(expIgnoreCase.toString()).isEqualTo(expectedIgnoreCase);
    assertThat(phexp).isEqualTo(JsonArray.create().add("a"));
    assertThat(phexpNum).isEqualTo(JsonArray.create().add(1));
    assertThat(phexpIgnoreCase).isEqualTo(JsonArray.create().add("b"));
  }

  @Test
  public void testAFTER() throws Exception {
    Part.Type keyword = Part.Type.AFTER;
    Iterator<Object> values = Arrays.<Object>asList("a", 1, "b").iterator();
    String expected = "doc.field > $0";
    String expectedNum = "doc.field > $0";
    String expectedIgnoreCase = "LOWER(doc.field) > LOWER($0)";

    JsonArray phexp = JsonArray.create();
    Expression exp = N1qlQueryCreatorUtils.createExpression(keyword, "doc.field", false, values, new AtomicInteger(), phexp);
    JsonArray phexpNum = JsonArray.create();
    Expression expNum = N1qlQueryCreatorUtils.createExpression(keyword, "doc.field", false, values, new AtomicInteger(), phexpNum);
    JsonArray phexpIgnoreCase = JsonArray.create();
    Expression expIgnoreCase = N1qlQueryCreatorUtils.createExpression(keyword, "doc.field", true, values, new AtomicInteger(), phexpIgnoreCase);

    assertThat(exp.toString()).isEqualTo(expected);
    assertThat(expNum.toString()).isEqualTo(expectedNum);
    assertThat(expIgnoreCase.toString()).isEqualTo(expectedIgnoreCase);
    assertThat(phexp).isEqualTo(JsonArray.create().add("a"));
    assertThat(phexpNum).isEqualTo(JsonArray.create().add(1));
    assertThat(phexpIgnoreCase).isEqualTo(JsonArray.create().add("b"));
  }

  @Test
  public void testNOT_LIKE() throws Exception {
    Part.Type keyword = Part.Type.NOT_LIKE;
    Iterator<Object> values = Arrays.<Object>asList("a", 1, "b").iterator();
    String expected = "doc.field NOT LIKE $0";
    String expectedNum = "doc.field NOT LIKE $0";
    String expectedIgnoreCase = "LOWER(doc.field) NOT LIKE LOWER($0)";

    JsonArray phexp = JsonArray.create();
    Expression exp = N1qlQueryCreatorUtils.createExpression(keyword, "doc.field", false, values, new AtomicInteger(), phexp);
    JsonArray phexpNum = JsonArray.create();
    Expression expNum = N1qlQueryCreatorUtils.createExpression(keyword, "doc.field", false, values, new AtomicInteger(), phexpNum);
    JsonArray phexpIgnoreCase = JsonArray.create();
    Expression expIgnoreCase = N1qlQueryCreatorUtils.createExpression(keyword, "doc.field", true, values, new AtomicInteger(), phexpIgnoreCase);

    assertThat(exp.toString()).isEqualTo(expected);
    assertThat(expNum.toString()).isEqualTo(expectedNum);
    assertThat(expIgnoreCase.toString()).isEqualTo(expectedIgnoreCase);
    assertThat(phexp).isEqualTo(JsonArray.create().add("a"));
    assertThat(phexpNum).isEqualTo(JsonArray.create().add(1));
    assertThat(phexpIgnoreCase).isEqualTo(JsonArray.create().add("b"));
  }

  @Test
  public void testLIKE() throws Exception {
    Part.Type keyword = Part.Type.LIKE;
    Iterator<Object> values = Arrays.<Object>asList("a", 1, "b").iterator();
    String expected = "doc.field LIKE $0";
    String expectedNum = "doc.field LIKE $0";
    String expectedIgnoreCase = "LOWER(doc.field) LIKE LOWER($0)";

    JsonArray phexp = JsonArray.create();
    Expression exp = N1qlQueryCreatorUtils.createExpression(keyword, "doc.field", false, values, new AtomicInteger(), phexp);
    JsonArray phexpNum = JsonArray.create();
    Expression expNum = N1qlQueryCreatorUtils.createExpression(keyword, "doc.field", false, values, new AtomicInteger(), phexpNum);
    JsonArray phexpIgnoreCase = JsonArray.create();
    Expression expIgnoreCase = N1qlQueryCreatorUtils.createExpression(keyword, "doc.field", true, values, new AtomicInteger(), phexpIgnoreCase);

    assertThat(exp.toString()).isEqualTo(expected);
    assertThat(expNum.toString()).isEqualTo(expectedNum);
    assertThat(expIgnoreCase.toString()).isEqualTo(expectedIgnoreCase);
    assertThat(phexp).isEqualTo(JsonArray.create().add("a"));
    assertThat(phexpNum).isEqualTo(JsonArray.create().add(1));
    assertThat(phexpIgnoreCase).isEqualTo(JsonArray.create().add("b"));
  }

  @Test
  public void testSTARTING_WITH() throws Exception {
    Part.Type keyword = Part.Type.STARTING_WITH;
    Iterator<Object> values = Arrays.<Object>asList("a", 1, "b").iterator();
    String expected = "doc.field LIKE $0 || '%'";
    String expectedNum = "doc.field LIKE $0 || '%'";
    String expectedIgnoreCase = "LOWER(doc.field) LIKE LOWER($0) || '%'";

    JsonArray phexp = JsonArray.create();
    Expression exp = N1qlQueryCreatorUtils.createExpression(keyword, "doc.field", false, values, new AtomicInteger(), phexp);
    JsonArray phexpNum = JsonArray.create();
    Expression expNum = N1qlQueryCreatorUtils.createExpression(keyword, "doc.field", false, values, new AtomicInteger(), phexpNum);
    JsonArray phexpIgnoreCase = JsonArray.create();
    Expression expIgnoreCase = N1qlQueryCreatorUtils.createExpression(keyword, "doc.field", true, values, new AtomicInteger(), phexpIgnoreCase);

    assertThat(exp.toString()).isEqualTo(expected);
    assertThat(expNum.toString()).isEqualTo(expectedNum);
    assertThat(expIgnoreCase.toString()).isEqualTo(expectedIgnoreCase);
    assertThat(phexp).isEqualTo(JsonArray.create().add("a"));
    assertThat(phexpNum).isEqualTo(JsonArray.create().add(1));
    assertThat(phexpIgnoreCase).isEqualTo(JsonArray.create().add("b"));
  }

  @Test
  public void testENDING_WITH() throws Exception {
    Part.Type keyword = Part.Type.ENDING_WITH;
    Iterator<Object> values = Arrays.<Object>asList("a", 1, "b").iterator();
    String expected = "doc.field LIKE '%' || $0";
    String expectedNum = "doc.field LIKE '%' || $0";
    String expectedIgnoreCase = "LOWER(doc.field) LIKE '%' || LOWER($0)";

    JsonArray phexp = JsonArray.create();
    Expression exp = N1qlQueryCreatorUtils.createExpression(keyword, "doc.field", false, values, new AtomicInteger(), phexp);
    JsonArray phexpNum = JsonArray.create();
    Expression expNum = N1qlQueryCreatorUtils.createExpression(keyword, "doc.field", false, values, new AtomicInteger(), phexpNum);
    JsonArray phexpIgnoreCase = JsonArray.create();
    Expression expIgnoreCase = N1qlQueryCreatorUtils.createExpression(keyword, "doc.field", true, values, new AtomicInteger(), phexpIgnoreCase);

    assertThat(exp.toString()).isEqualTo(expected);
    assertThat(expNum.toString()).isEqualTo(expectedNum);
    assertThat(expIgnoreCase.toString()).isEqualTo(expectedIgnoreCase);
    assertThat(phexp).isEqualTo(JsonArray.create().add("a"));
    assertThat(phexpNum).isEqualTo(JsonArray.create().add(1));
    assertThat(phexpIgnoreCase).isEqualTo(JsonArray.create().add("b"));
  }

  @Test
  public void testNOT_CONTAINING() throws Exception {
    Part.Type keyword = Part.Type.NOT_CONTAINING;
    Iterator<Object> values = Arrays.<Object>asList("a", 1, "b").iterator();
    String expected = "doc.field NOT LIKE '%' || $0 || '%'";
    String expectedNum = "doc.field NOT LIKE '%' || $0 || '%'";
    String expectedIgnoreCase = "LOWER(doc.field) NOT LIKE '%' || LOWER($0) || '%'";

    JsonArray phexp = JsonArray.create();
    Expression exp = N1qlQueryCreatorUtils.createExpression(keyword, "doc.field", false, values, new AtomicInteger(), phexp);
    JsonArray phexpNum = JsonArray.create();
    Expression expNum = N1qlQueryCreatorUtils.createExpression(keyword, "doc.field", false, values, new AtomicInteger(), phexpNum);
    JsonArray phexpIgnoreCase = JsonArray.create();
    Expression expIgnoreCase = N1qlQueryCreatorUtils.createExpression(keyword, "doc.field", true, values, new AtomicInteger(), phexpIgnoreCase);

    assertThat(exp.toString()).isEqualTo(expected);
    assertThat(expNum.toString()).isEqualTo(expectedNum);
    assertThat(expIgnoreCase.toString()).isEqualTo(expectedIgnoreCase);
    assertThat(phexp).isEqualTo(JsonArray.create().add("a"));
    assertThat(phexpNum).isEqualTo(JsonArray.create().add(1));
    assertThat(phexpIgnoreCase).isEqualTo(JsonArray.create().add("b"));
  }

  @Test
  public void testCONTAINING() throws Exception {
    Part.Type keyword = Part.Type.CONTAINING;
    Iterator<Object> values = Arrays.<Object>asList("a", 1, "b").iterator();
    String expected = "doc.field LIKE '%' || $0 || '%'";
    String expectedNum = "doc.field LIKE '%' || $0 || '%'";
    String expectedIgnoreCase = "LOWER(doc.field) LIKE '%' || LOWER($0) || '%'";

    JsonArray phexp = JsonArray.create();
    Expression exp = N1qlQueryCreatorUtils.createExpression(keyword, "doc.field", false, values, new AtomicInteger(), phexp);
    JsonArray phexpNum = JsonArray.create();
    Expression expNum = N1qlQueryCreatorUtils.createExpression(keyword, "doc.field", false, values, new AtomicInteger(), phexpNum);
    JsonArray phexpIgnoreCase = JsonArray.create();
    Expression expIgnoreCase = N1qlQueryCreatorUtils.createExpression(keyword, "doc.field", true, values, new AtomicInteger(), phexpIgnoreCase);

    assertThat(exp.toString()).isEqualTo(expected);
    assertThat(expNum.toString()).isEqualTo(expectedNum);
    assertThat(expIgnoreCase.toString()).isEqualTo(expectedIgnoreCase);
    assertThat(phexp).isEqualTo(JsonArray.create().add("a"));
    assertThat(phexpNum).isEqualTo(JsonArray.create().add(1));
    assertThat(phexpIgnoreCase).isEqualTo(JsonArray.create().add("b"));
  }

  @Test
  public void testNOT_IN() throws Exception {
    Part.Type keyword = Part.Type.NOT_IN;
    Iterator<Object> values = Arrays.<Object>asList("a", 1, "b").iterator();
    String expected = "doc.field NOT IN $0";
    String expectedNum = "doc.field NOT IN $0";
    String expectedIgnoreCase = "LOWER(doc.field) NOT IN $0";

    JsonArray phexp = JsonArray.create();
    Expression exp = N1qlQueryCreatorUtils.createExpression(keyword, "doc.field", false, values, new AtomicInteger(), phexp);
    JsonArray phexpNum = JsonArray.create();
    Expression expNum = N1qlQueryCreatorUtils.createExpression(keyword, "doc.field", false, values, new AtomicInteger(), phexpNum);
    JsonArray phexpIgnoreCase = JsonArray.create();
    Expression expIgnoreCase = N1qlQueryCreatorUtils.createExpression(keyword, "doc.field", true, values, new AtomicInteger(), phexpIgnoreCase);

    assertThat(exp.toString()).isEqualTo(expected);
    assertThat(expNum.toString()).isEqualTo(expectedNum);
    assertThat(expIgnoreCase.toString()).isEqualTo(expectedIgnoreCase);
    assertThat(phexp).isEqualTo(JsonArray.create().add(JsonArray.create().add("a")));
    assertThat(phexpNum).isEqualTo(JsonArray.create().add(JsonArray.create().add(1)));
    assertThat(phexpIgnoreCase)
			.isEqualTo(JsonArray.create().add(JsonArray.create().add("b")));
  }

  @Test
  public void testNOTINwithCollection() throws Exception {
    Part.Type keyword = Part.Type.NOT_IN;
    List<Object> val1 = Arrays.<Object>asList("av1", "av2");
    List<Object> val2 = Arrays.<Object>asList("bv1", "bv2");
    Iterator<Object> values = Arrays.<Object>asList(val1, val2).iterator();
    String expected = "doc.field NOT IN $0";
    String expectedIgnoreCase = "LOWER(doc.field) NOT IN $0";

    JsonArray phexp = JsonArray.create();
    Expression exp = N1qlQueryCreatorUtils.createExpression(keyword, "doc.field", false, values, new AtomicInteger(), phexp);
    JsonArray phexpIgnoreCase = JsonArray.create();
    Expression expIgnoreCase = N1qlQueryCreatorUtils.createExpression(keyword, "doc.field", true, values, new AtomicInteger(), phexpIgnoreCase);

    assertThat(exp.toString()).isEqualTo(expected);
    assertThat(expIgnoreCase.toString()).isEqualTo(expectedIgnoreCase);
    assertThat(phexp)
			.isEqualTo(JsonArray.create().add(JsonArray.create().add("av1").add("av2")));
    assertThat(phexpIgnoreCase)
			.isEqualTo(JsonArray.create().add(JsonArray.create().add("bv1").add("bv2")));
  }

  @Test
  public void testNOTINwithArray() throws Exception {
    Part.Type keyword = Part.Type.NOT_IN;
    String[] val1 = {"av1", "av2"};
    String[] val2 = {"bv1", "bv2"};
    Iterator<Object> values = Arrays.<Object>asList(val1, val2).iterator();
    String expected = "doc.field NOT IN $0";
    String expectedIgnoreCase = "LOWER(doc.field) NOT IN $0";

    JsonArray phexp = JsonArray.create();
    Expression exp = N1qlQueryCreatorUtils.createExpression(keyword, "doc.field", false, values, new AtomicInteger(), phexp);
    JsonArray phexpIgnoreCase = JsonArray.create();
    Expression expIgnoreCase = N1qlQueryCreatorUtils.createExpression(keyword, "doc.field", true, values, new AtomicInteger(), phexpIgnoreCase);

    assertThat(exp.toString()).isEqualTo(expected);
    assertThat(expIgnoreCase.toString()).isEqualTo(expectedIgnoreCase);
    assertThat(phexp)
			.isEqualTo(JsonArray.create().add(JsonArray.create().add("av1").add("av2")));
    assertThat(phexpIgnoreCase)
			.isEqualTo(JsonArray.create().add(JsonArray.create().add("bv1").add("bv2")));
  }

  @Test
  public void testIN() throws Exception {
    Part.Type keyword = Part.Type.IN;
    Iterator<Object> values = Arrays.<Object>asList("a", 1, "b").iterator();
    String expected = "doc.field IN $0";
    String expectedNum = "doc.field IN $0";
    String expectedIgnoreCase = "LOWER(doc.field) IN $0";

    JsonArray phexp = JsonArray.create();
    Expression exp = N1qlQueryCreatorUtils.createExpression(keyword, "doc.field", false, values, new AtomicInteger(), phexp);
    JsonArray phexpNum = JsonArray.create();
    Expression expNum = N1qlQueryCreatorUtils.createExpression(keyword, "doc.field", false, values, new AtomicInteger(), phexpNum);
    JsonArray phexpIgnoreCase = JsonArray.create();
    Expression expIgnoreCase = N1qlQueryCreatorUtils.createExpression(keyword, "doc.field", true, values, new AtomicInteger(), phexpIgnoreCase);

    assertThat(exp.toString()).isEqualTo(expected);
    assertThat(expNum.toString()).isEqualTo(expectedNum);
    assertThat(expIgnoreCase.toString()).isEqualTo(expectedIgnoreCase);
    assertThat(phexp).isEqualTo(JsonArray.create().add(JsonArray.create().add("a")));
    assertThat(phexpNum).isEqualTo(JsonArray.create().add(JsonArray.create().add(1)));
    assertThat(phexpIgnoreCase)
			.isEqualTo(JsonArray.create().add(JsonArray.create().add("b")));
  }

  @Test
  public void testINwithCollection() throws Exception {
    Part.Type keyword = Part.Type.IN;
    List<Object> val1 = Arrays.<Object>asList("av1", "av2");
    List<Object> val2 = Arrays.<Object>asList("bv1", "bv2");
    Iterator<Object> values = Arrays.<Object>asList(val1, val2).iterator();
    String expected = "doc.field IN $0";
    String expectedIgnoreCase = "LOWER(doc.field) IN $0";

    JsonArray phexp = JsonArray.create();
    Expression exp = N1qlQueryCreatorUtils.createExpression(keyword, "doc.field", false, values, new AtomicInteger(), phexp);
    JsonArray phexpIgnoreCase = JsonArray.create();
    Expression expIgnoreCase = N1qlQueryCreatorUtils.createExpression(keyword, "doc.field", true, values, new AtomicInteger(), phexpIgnoreCase);

    assertThat(exp.toString()).isEqualTo(expected);
    assertThat(expIgnoreCase.toString()).isEqualTo(expectedIgnoreCase);
    assertThat(phexp)
			.isEqualTo(JsonArray.create().add(JsonArray.create().add("av1").add("av2")));
    assertThat(phexpIgnoreCase)
			.isEqualTo(JsonArray.create().add(JsonArray.create().add("bv1").add("bv2")));
  }

  @Test
  public void testINwithArray() throws Exception {
    Part.Type keyword = Part.Type.IN;
    String[] val1 = {"av1", "av2"};
    String[] val2 = {"bv1", "bv2"};
    Iterator<Object> values = Arrays.<Object>asList(val1, val2).iterator();
    String expected = "doc.field IN $0";
    String expectedIgnoreCase = "LOWER(doc.field) IN $0";

    JsonArray phexp = JsonArray.create();
    Expression exp = N1qlQueryCreatorUtils.createExpression(keyword, "doc.field", false, values, new AtomicInteger(), phexp);
    JsonArray phexpIgnoreCase = JsonArray.create();
    Expression expIgnoreCase = N1qlQueryCreatorUtils.createExpression(keyword, "doc.field", true, values, new AtomicInteger(), phexpIgnoreCase);

    assertThat(exp.toString()).isEqualTo(expected);
    assertThat(expIgnoreCase.toString()).isEqualTo(expectedIgnoreCase);
    assertThat(phexp)
			.isEqualTo(JsonArray.create().add(JsonArray.create().add("av1").add("av2")));
    assertThat(phexpIgnoreCase)
			.isEqualTo(JsonArray.create().add(JsonArray.create().add("bv1").add("bv2")));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNEAR() throws Exception {
    Part.Type keyword = Part.Type.NEAR;
    Iterator<Object> values = Arrays.<Object>asList("a", 1, "b").iterator();

    Expression exp = N1qlQueryCreatorUtils.createExpression(keyword, "doc.field", true, values, new AtomicInteger(), JsonArray.create());;
  }

  @Test(expected = IllegalArgumentException.class)
  public void testWITHIN() throws Exception {
    Part.Type keyword = Part.Type.WITHIN;
    Iterator<Object> values = Arrays.<Object>asList("a", 1, "b").iterator();

    Expression exp = N1qlQueryCreatorUtils.createExpression(keyword, "doc.field", false, values, new AtomicInteger(), JsonArray.create());;
  }

  @Test
  public void testREGEX() throws Exception {
    Part.Type keyword = Part.Type.REGEX;
    Iterator<Object> values = Arrays.<Object>asList("a", 1, "b").iterator();
    String expected = "REGEXP_LIKE(doc.field, $0)";
    String expectedNum = "REGEXP_LIKE(doc.field, $0)";
    String expectedIgnoreCase = "REGEXP_LIKE(LOWER(doc.field), $0)";

    JsonArray phexp = JsonArray.create();
    Expression exp = N1qlQueryCreatorUtils.createExpression(keyword, "doc.field", false, values, new AtomicInteger(), phexp);
    JsonArray phexpNum = JsonArray.create();
    Expression expNum = N1qlQueryCreatorUtils.createExpression(keyword, "doc.field", false, values, new AtomicInteger(), phexpNum);
    JsonArray phexpIgnoreCase = JsonArray.create();
    Expression expIgnoreCase = N1qlQueryCreatorUtils.createExpression(keyword, "doc.field", true, values, new AtomicInteger(), phexpIgnoreCase);

    assertThat(exp.toString()).isEqualTo(expected);
    assertThat(expNum.toString()).isEqualTo(expectedNum);
    assertThat(expIgnoreCase.toString()).isEqualTo(expectedIgnoreCase);
    assertThat(phexp).isEqualTo(JsonArray.create().add("a"));
    assertThat(phexpNum).isEqualTo(JsonArray.create().add("1"));
    assertThat(phexpIgnoreCase).isEqualTo(JsonArray.create().add("b"));
  }

  @Test
  public void testEXISTS() throws Exception {
    Part.Type keyword = Part.Type.EXISTS;
    Iterator<Object> values = Arrays.<Object>asList("a", 1, "b").iterator();
    String expected = "doc.field IS NOT MISSING";
    String expectedNum = "doc.field IS NOT MISSING";
    String expectedIgnoreCase = "LOWER(doc.field) IS NOT MISSING";

    JsonArray phexp = JsonArray.create();
    Expression exp = N1qlQueryCreatorUtils.createExpression(keyword, "doc.field", false, values, new AtomicInteger(), phexp);
    JsonArray phexpNum = JsonArray.create();
    Expression expNum = N1qlQueryCreatorUtils.createExpression(keyword, "doc.field", false, values, new AtomicInteger(), phexpNum);
    JsonArray phexpIgnoreCase = JsonArray.create();
    Expression expIgnoreCase = N1qlQueryCreatorUtils.createExpression(keyword, "doc.field", true, values, new AtomicInteger(), phexpIgnoreCase);

    assertThat(exp.toString()).isEqualTo(expected);
    assertThat(expNum.toString()).isEqualTo(expectedNum);
    assertThat(expIgnoreCase.toString()).isEqualTo(expectedIgnoreCase);
    assertThat(phexp).isEqualTo(JsonArray.create());
    assertThat(phexpNum).isEqualTo(JsonArray.create());
    assertThat(phexpIgnoreCase).isEqualTo(JsonArray.create());
  }

  @Test
  public void testTRUE() throws Exception {
    Part.Type keyword = Part.Type.TRUE;
    Iterator<Object> values = Arrays.<Object>asList("a", 1, "b").iterator();
    String expected = "doc.field = TRUE";
    String expectedNum = "doc.field = TRUE";
    String expectedIgnoreCase = "LOWER(doc.field) = TRUE";

    JsonArray phexp = JsonArray.create();
    Expression exp = N1qlQueryCreatorUtils.createExpression(keyword, "doc.field", false, values, new AtomicInteger(), phexp);
    JsonArray phexpNum = JsonArray.create();
    Expression expNum = N1qlQueryCreatorUtils.createExpression(keyword, "doc.field", false, values, new AtomicInteger(), phexpNum);
    JsonArray phexpIgnoreCase = JsonArray.create();
    Expression expIgnoreCase = N1qlQueryCreatorUtils.createExpression(keyword, "doc.field", true, values, new AtomicInteger(), phexpIgnoreCase);

    assertThat(exp.toString()).isEqualTo(expected);
    assertThat(expNum.toString()).isEqualTo(expectedNum);
    assertThat(expIgnoreCase.toString()).isEqualTo(expectedIgnoreCase);
    assertThat(phexp).isEqualTo(JsonArray.create());
    assertThat(phexpNum).isEqualTo(JsonArray.create());
    assertThat(phexpIgnoreCase).isEqualTo(JsonArray.create());
  }

  @Test
  public void testFALSE() throws Exception {
    Part.Type keyword = Part.Type.FALSE;
    Iterator<Object> values = Arrays.<Object>asList("a", 1, "b").iterator();
    String expected = "doc.field = FALSE";
    String expectedNum = "doc.field = FALSE";
    String expectedIgnoreCase = "LOWER(doc.field) = FALSE";

    JsonArray phexp = JsonArray.create();
    Expression exp = N1qlQueryCreatorUtils.createExpression(keyword, "doc.field", false, values, new AtomicInteger(), phexp);
    JsonArray phexpNum = JsonArray.create();
    Expression expNum = N1qlQueryCreatorUtils.createExpression(keyword, "doc.field", false, values, new AtomicInteger(), phexpNum);
    JsonArray phexpIgnoreCase = JsonArray.create();
    Expression expIgnoreCase = N1qlQueryCreatorUtils.createExpression(keyword, "doc.field", true, values, new AtomicInteger(), phexpIgnoreCase);

    assertThat(exp.toString()).isEqualTo(expected);
    assertThat(expNum.toString()).isEqualTo(expectedNum);
    assertThat(expIgnoreCase.toString()).isEqualTo(expectedIgnoreCase);
    assertThat(phexp).isEqualTo(JsonArray.create());
    assertThat(phexpNum).isEqualTo(JsonArray.create());
    assertThat(phexpIgnoreCase).isEqualTo(JsonArray.create());
  }

  @Test
  public void testNEGATING_SIMPLE_PROPERTY() throws Exception {
    Part.Type keyword = Part.Type.NEGATING_SIMPLE_PROPERTY;
    Iterator<Object> values = Arrays.<Object>asList("a", 1, "b").iterator();
    String expected = "doc.field != $0";
    String expectedNum = "doc.field != $0";
    String expectedIgnoreCase = "LOWER(doc.field) != LOWER($0)";

    JsonArray phexp = JsonArray.create();
    Expression exp = N1qlQueryCreatorUtils.createExpression(keyword, "doc.field", false, values, new AtomicInteger(), phexp);
    JsonArray phexpNum = JsonArray.create();
    Expression expNum = N1qlQueryCreatorUtils.createExpression(keyword, "doc.field", false, values, new AtomicInteger(), phexpNum);
    JsonArray phexpIgnoreCase = JsonArray.create();
    Expression expIgnoreCase = N1qlQueryCreatorUtils.createExpression(keyword, "doc.field", true, values, new AtomicInteger(), phexpIgnoreCase);

    assertThat(exp.toString()).isEqualTo(expected);
    assertThat(expNum.toString()).isEqualTo(expectedNum);
    assertThat(expIgnoreCase.toString()).isEqualTo(expectedIgnoreCase);
    assertThat(phexp).isEqualTo(JsonArray.create().add("a"));
    assertThat(phexpNum).isEqualTo(JsonArray.create().add(1));
    assertThat(phexpIgnoreCase).isEqualTo(JsonArray.create().add("b"));
  }

  enum TestEnum {
    TEST
  }

  @Test
  public void testSIMPLE_PROPERTY() throws Exception {
    Part.Type keyword = Part.Type.SIMPLE_PROPERTY;
    Iterator<Object> values = Arrays.<Object>asList("a", 1, "b", TestEnum.TEST).iterator();
    String expected = "doc.field = $0";
    String expectedNum = "doc.field = $0";
    String expectedIgnoreCase = "LOWER(doc.field) = LOWER($0)";
    String expectedEnum = "doc.field = $0";

    JsonArray phexp = JsonArray.create();
    Expression exp = N1qlQueryCreatorUtils.createExpression(keyword, "doc.field", false, values, new AtomicInteger(), phexp);
    JsonArray phexpNum = JsonArray.create();
    Expression expNum = N1qlQueryCreatorUtils.createExpression(keyword, "doc.field", false, values, new AtomicInteger(), phexpNum);
    JsonArray phexpIgnoreCase = JsonArray.create();
    Expression expIgnoreCase = N1qlQueryCreatorUtils.createExpression(keyword, "doc.field", true, values, new AtomicInteger(), phexpIgnoreCase);
    JsonArray phexpEnum = JsonArray.create();
    Expression expEnum = N1qlQueryCreatorUtils.createExpression(keyword, "doc.field", false, values, new AtomicInteger(), phexpEnum);

    assertThat(exp.toString()).isEqualTo(expected);
    assertThat(expNum.toString()).isEqualTo(expectedNum);
    assertThat(expIgnoreCase.toString()).isEqualTo(expectedIgnoreCase);
    assertThat(expEnum.toString()).isEqualTo(expectedEnum);
    assertThat(phexp).isEqualTo(JsonArray.create().add("a"));
    assertThat(phexpNum).isEqualTo(JsonArray.create().add(1));
    assertThat(phexpIgnoreCase).isEqualTo(JsonArray.create().add("b"));
    assertThat(phexpEnum).isEqualTo(JsonArray.create().add("TEST"));
  }
}
