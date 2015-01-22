/*
 * Copyright 2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.couchbase.core.mapping;

import org.joda.time.LocalDateTime;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.annotation.Id;
import org.springframework.data.couchbase.TestApplicationConfig;
import org.springframework.data.couchbase.core.convert.MappingCouchbaseConverter;
import org.springframework.data.mapping.model.MappingException;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.data.couchbase.core.convert.CustomConversions;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * @author Michael Nitschinger
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = TestApplicationConfig.class)
public class MappingCouchbaseConverterTests {

  @Autowired
  private MappingCouchbaseConverter converter;

  @Test
  public void shouldNotThrowNPE() {
    CouchbaseDocument converted = new CouchbaseDocument();
    converter.write(null, converted);
    assertNull(converted.getId());
    assertEquals(0, converted.getExpiration());
  }

  @Test(expected = MappingException.class)
  public void doesNotAllowSimpleType1() {
    converter.write("hello", new CouchbaseDocument());
  }

  @Test(expected = MappingException.class)
  public void doesNotAllowSimpleType2() {
    converter.write(true, new CouchbaseDocument());
  }

  @Test(expected = MappingException.class)
  public void doesNotAllowSimpleType3() {
    converter.write(42, new CouchbaseDocument());
  }

  @Test(expected = MappingException.class)
  public void needsIDOnEntity() {
    converter.write(new EntityWithoutID("foo"),
      new CouchbaseDocument());
  }

  @Test
  public void writesString() throws Exception {
    CouchbaseDocument converted = new CouchbaseDocument();
    StringEntity entity = new StringEntity("foobar");

    converter.write(entity, converted);
    Map<String, Object> result = converted.export();
    assertEquals(entity.getClass().getName(), result.get("_class"));
    assertEquals("foobar", result.get("attr0"));
    assertEquals(BaseEntity.ID, converted.getId());
  }

  @Test
  public void readsString() {
    CouchbaseDocument source = new CouchbaseDocument();
    source.put("_class", StringEntity.class.getName());
    source.put("attr0", "foobar");

    StringEntity converted = converter.read(StringEntity.class, source);
    assertEquals("foobar", converted.attr0);
  }

  @Test
  public void writesNumber() {
    CouchbaseDocument converted = new CouchbaseDocument();
    NumberEntity entity = new NumberEntity(42);

    converter.write(entity, converted);
    Map<String, Object> result = converted.export();
    assertEquals(entity.getClass().getName(), result.get("_class"));
    assertEquals(42L, result.get("attr0"));
    assertEquals(BaseEntity.ID, converted.getId());
  }

  @Test
  public void readsNumber() {
    CouchbaseDocument source = new CouchbaseDocument();
    source.put("_class", NumberEntity.class.getName());
    source.put("attr0", 42);

    NumberEntity converted = converter.read(NumberEntity.class, source);
    assertEquals(42, converted.attr0);
  }

  @Test
  public void writesBoolean() {
    CouchbaseDocument converted = new CouchbaseDocument();
    BooleanEntity entity = new BooleanEntity(true);

    converter.write(entity, converted);
    Map<String, Object> result = converted.export();
    assertEquals(entity.getClass().getName(), result.get("_class"));
    assertEquals(true, result.get("attr0"));
    assertEquals("mockid", converted.getId());
  }

  @Test
  public void readsBoolean() {
    CouchbaseDocument source = new CouchbaseDocument();
    source.put("_class", BooleanEntity.class.getName());
    source.put("attr0", true);

    BooleanEntity converted = converter.read(BooleanEntity.class, source);
    assertTrue(converted.attr0);
  }

  @Test
  public void writesMixedSimpleTypes() {
    CouchbaseDocument converted = new CouchbaseDocument();
    MixedSimpleEntity entity = new MixedSimpleEntity("a", 5, -0.3, true);

    converter.write(entity, converted);
    Map<String, Object> result = converted.export();
    assertEquals(entity.getClass().getName(), result.get("_class"));
    assertEquals("a", result.get("attr0"));
    assertEquals(5, result.get("attr1"));
    assertEquals(-0.3, result.get("attr2"));
    assertEquals(true, result.get("attr3"));
  }

  @Test
  public void readsMixedSimpleTypes() {
    CouchbaseDocument source = new CouchbaseDocument();
    source.put("_class", MixedSimpleEntity.class.getName());
    source.put("attr0", "a");
    source.put("attr1", 5);
    source.put("attr2", -0.3);
    source.put("attr3", true);

    MixedSimpleEntity converted = converter.read(MixedSimpleEntity.class, source);
    assertEquals("a", converted.attr0);
    assertEquals(5, converted.attr1);
    assertEquals(-0.3, converted.attr2, 0);
    assertTrue(converted.attr3);
  }

  @Test
  public void readsID() {
    CouchbaseDocument document = new CouchbaseDocument("001");

    BasicCouchbasePersistentPropertyTests.Beer beer = converter.read(BasicCouchbasePersistentPropertyTests.Beer.class,
        document);

    assertEquals("001", beer.getId());
  }

  @Test
  public void writesUninitializedValues() {
    CouchbaseDocument converted = new CouchbaseDocument();
    UninitializedEntity entity = new UninitializedEntity();

    converter.write(entity, converted);
    Map<String, Object> result = converted.export();
    assertEquals(entity.getClass().getName(), result.get("_class"));
    assertEquals(0, result.get("attr1"));
  }

  @Test
  public void readsUninitializedValues() {
    CouchbaseDocument source = new CouchbaseDocument();
    source.put("_class", UninitializedEntity.class.getName());
    source.put("attr1", 0);

    UninitializedEntity converted = converter.read(UninitializedEntity.class, source);
    assertNull(converted.attr0);
    assertEquals(0, converted.attr1);
    assertNull(converted.attr2);
  }

  @Test
  public void writesAndReadsMapsAndNestedMaps() {
    CouchbaseDocument converted = new CouchbaseDocument();

    Map<String, String> attr0 = new HashMap<String, String>();
    Map<String, Boolean> attr1 = new TreeMap<String, Boolean>();
    Map<Integer, String> attr2 = new LinkedHashMap<Integer, String>();
    Map<String, Map<String, String>> attr3 =
      new HashMap<String, Map<String, String>>();

    attr0.put("foo", "bar");
    attr1.put("bar", true);
    attr3.put("hashmap", attr0);

    MapEntity entity = new MapEntity(attr0, attr1, attr2, attr3);

    converter.write(entity, converted);
    Map<String, Object> result = converted.export();
    assertEquals(attr0, result.get("attr0"));
    assertEquals(attr1, result.get("attr1"));
    assertEquals(attr2, result.get("attr2"));
    assertEquals(attr3, result.get("attr3"));

    CouchbaseDocument cattr0 = new CouchbaseDocument();
    cattr0.put("foo", "bar");

    CouchbaseDocument cattr1 = new CouchbaseDocument();
    cattr1.put("bar", true);

    CouchbaseDocument cattr2 = new CouchbaseDocument();

    CouchbaseDocument cattr3 = new CouchbaseDocument();
    cattr3.put("hashmap", cattr0);

    CouchbaseDocument source = new CouchbaseDocument();
    source.put("_class", MapEntity.class.getName());
    source.put("attr0", cattr0);
    source.put("attr1", cattr1);
    source.put("attr2", cattr2);
    source.put("attr3", cattr3);

    MapEntity readConverted = converter.read(MapEntity.class, source);
    assertEquals(attr0, readConverted.attr0);
    assertEquals(attr1, readConverted.attr1);
    assertEquals(attr2, readConverted.attr2);
    assertEquals(attr3, readConverted.attr3);
  }

  @Test
  public void writesAndReadsListAndNestedList() {
    CouchbaseDocument converted = new CouchbaseDocument();
    List<String> attr0 = new ArrayList<String>();
    List<Integer> attr1 = new LinkedList<Integer>();
    List<List<String>> attr2 = new ArrayList<List<String>>();

    attr0.add("foo");
    attr0.add("bar");
    attr2.add(attr0);

    ListEntity entity = new ListEntity(attr0, attr1, attr2);

    converter.write(entity, converted);
    Map<String, Object> result = converted.export();
    assertEquals(attr0, result.get("attr0"));
    assertEquals(attr1, result.get("attr1"));
    assertEquals(attr2, result.get("attr2"));

    CouchbaseDocument source = new CouchbaseDocument();
    source.put("_class", ListEntity.class.getName());
    CouchbaseList cattr0 = new CouchbaseList();
    cattr0.put("foo");
    cattr0.put("bar");
    CouchbaseList cattr1 = new CouchbaseList();
    CouchbaseList cattr2 = new CouchbaseList();
    cattr2.put(cattr0);
    source.put("attr0", cattr0);
    source.put("attr1", cattr1);
    source.put("attr2", cattr2);

    ListEntity readConverted = converter.read(ListEntity.class, source);
    assertEquals(2, readConverted.attr0.size());
    assertEquals(0, readConverted.attr1.size());
    assertEquals(1, readConverted.attr2.size());
    assertEquals(2, readConverted.attr2.get(0).size());
  }

  @Test
  public void writesAndReadsSetAndNestedSet() {
    CouchbaseDocument converted = new CouchbaseDocument();
    Set<String> attr0 = new HashSet<String>();
    TreeSet<Integer> attr1 = new TreeSet<Integer>();
    Set<Set<String>> attr2 = new HashSet<Set<String>>();

    attr0.add("foo");
    attr0.add("bar");
    attr2.add(attr0);

    SetEntity entity = new SetEntity(attr0, attr1, attr2);

    converter.write(entity, converted);
    Map<String, Object> result = converted.export();
    assertEquals(attr0.size(), ((Collection) result.get("attr0")).size());
    assertEquals(attr1.size(), ((Collection) result.get("attr1")).size());
    assertEquals(attr2.size(), ((Collection) result.get("attr2")).size());

    CouchbaseList cattr0 = new CouchbaseList();
    cattr0.put("foo");
    cattr0.put("bar");

    CouchbaseList cattr1 = new CouchbaseList();

    CouchbaseList cattr2 = new CouchbaseList();
    cattr2.put(cattr0);

    CouchbaseDocument source = new CouchbaseDocument();
    source.put("_class", SetEntity.class.getName());
    source.put("attr0", cattr0);
    source.put("attr1", cattr1);
    source.put("attr2", cattr2);

    SetEntity readConverted = converter.read(SetEntity.class, source);
    assertEquals(attr0, readConverted.attr0);
    assertEquals(attr1, readConverted.attr1);
    assertEquals(attr2, readConverted.attr2);
  }

  @Test
  public void writesAndReadsValueClass() {
    CouchbaseDocument converted = new CouchbaseDocument();

    final String email = "foo@bar.com";
    final Email addy = new Email(email);
    List<Email> listOfEmails = new ArrayList<Email>();
    listOfEmails.add(addy);

    ValueEntity entity = new ValueEntity(addy, listOfEmails);
    converter.write(entity, converted);
    Map<String, Object> result = converted.export();

    assertEquals(entity.getClass().getName(), result.get("_class"));
    assertEquals(new HashMap<String, Object>() {{
      put("emailAddr", email);
    }}, result.get("email"));

    CouchbaseDocument source = new CouchbaseDocument();
    source.put("_class", ValueEntity.class.getName());
    CouchbaseDocument emailDoc = new CouchbaseDocument();
    emailDoc.put("emailAddr", "foo@bar.com");
    source.put("email", emailDoc);
    CouchbaseList listOfEmailsDoc = new CouchbaseList();
    listOfEmailsDoc.put(emailDoc);
    source.put("listOfEmails", listOfEmailsDoc);

    ValueEntity readConverted = converter.read(ValueEntity.class, source);
    assertEquals(addy.emailAddr, readConverted.email.emailAddr);
    assertEquals(listOfEmails.get(0).emailAddr,
      readConverted.listOfEmails.get(0).emailAddr);
  }

  @Test
  public void writesAndReadsCustomConvertedClass() {
    List<Object> converters = new ArrayList<Object>();
    converters.add(BigDecimalToStringConverter.INSTANCE);
    converters.add(StringToBigDecimalConverter.INSTANCE);
    converter.setCustomConversions(new CustomConversions(converters));
    converter.afterPropertiesSet();

    CouchbaseDocument converted = new CouchbaseDocument();

    final String valueStr = "12.345";
    final BigDecimal value = new BigDecimal(valueStr);
    final String value2Str = "0.6789";
    final BigDecimal value2 = new BigDecimal(value2Str);
    List<BigDecimal> listOfValues = new ArrayList<BigDecimal>();
    listOfValues.add(value);
    listOfValues.add(value2);
    Map<String, BigDecimal> mapOfValues = new HashMap<String, BigDecimal>();
    mapOfValues.put("val1", value);
    mapOfValues.put("val2", value2);

    CustomEntity entity = new CustomEntity(value, listOfValues, mapOfValues);
    converter.write(entity, converted);

    CouchbaseDocument source = new CouchbaseDocument();
    source.put("_class", CustomEntity.class.getName());
    source.put("value", valueStr);
    CouchbaseList listOfValuesDoc = new CouchbaseList();
    listOfValuesDoc.put(valueStr);
    listOfValuesDoc.put(value2Str);
    source.put("listOfValues", listOfValuesDoc);
    CouchbaseDocument mapOfValuesDoc = new CouchbaseDocument();
    mapOfValuesDoc.put("val1", valueStr);
    mapOfValuesDoc.put("val2", value2Str);
    source.put("mapOfValues", mapOfValuesDoc);
    assertEquals(source.export().toString(), converted.export().toString());

    CustomEntity readConverted = converter.read(CustomEntity.class, source);
    assertEquals(value, readConverted.value);
    assertEquals(listOfValues.get(0), readConverted.listOfValues.get(0));
    assertEquals(listOfValues.get(1), readConverted.listOfValues.get(1));
    assertEquals(mapOfValues.get("val1"), readConverted.mapOfValues.get("val1"));
    assertEquals(mapOfValues.get("val2"), readConverted.mapOfValues.get("val2"));
  }

  @Test
  public void writesAndReadsClassContainingCustomConvertedObjects() {
    List<Object> converters = new ArrayList<Object>();
    converters.add(BigDecimalToStringConverter.INSTANCE);
    converters.add(StringToBigDecimalConverter.INSTANCE);
    converter.setCustomConversions(new CustomConversions(converters));
    converter.afterPropertiesSet();

    CouchbaseDocument converted = new CouchbaseDocument();

    final String weightStr = "12.34";
    final BigDecimal weight = new BigDecimal(weightStr);
    final CustomObject addy = new CustomObject(weight);
    List<CustomObject> listOfObjects = new ArrayList<CustomObject>();
    listOfObjects.add(addy);
    Map<String, CustomObject> mapOfObjects = new HashMap<String, CustomObject>();
    mapOfObjects.put("obj0", addy);
    mapOfObjects.put("obj1", addy);

    CustomObjectEntity entity = new CustomObjectEntity(addy, listOfObjects, mapOfObjects);
    converter.write(entity, converted);

    CouchbaseDocument source = new CouchbaseDocument();
    source.put("_class", CustomObjectEntity.class.getName());
    CouchbaseDocument objectDoc = new CouchbaseDocument();
    objectDoc.put("weight", weightStr);
    source.put("object", objectDoc);
    CouchbaseList listOfObjectsDoc = new CouchbaseList();
    listOfObjectsDoc.put(objectDoc);
    source.put("listOfObjects", listOfObjectsDoc);
    CouchbaseDocument mapOfObjectsDoc = new CouchbaseDocument();
    mapOfObjectsDoc.put("obj0", objectDoc);
    mapOfObjectsDoc.put("obj1", objectDoc);
    source.put("mapOfObjects", mapOfObjectsDoc);
    assertEquals(source.export().toString(), converted.export().toString());

    CustomObjectEntity readConverted = converter.read(CustomObjectEntity.class, source);
    assertEquals(addy.weight, readConverted.object.weight);
    assertEquals(listOfObjects.get(0).weight, readConverted.listOfObjects.get(0).weight);
    assertEquals(mapOfObjects.get("obj0").weight, readConverted.mapOfObjects.get("obj0").weight);
    assertEquals(mapOfObjects.get("obj1").weight, readConverted.mapOfObjects.get("obj1").weight);
  }

  @Test
  public void writesAndReadsDates() {
    Date created = new Date();
    Calendar modified = Calendar.getInstance();
    LocalDateTime deleted = LocalDateTime.now();
    DateEntity entity = new DateEntity(created, modified, deleted);

    CouchbaseDocument converted = new CouchbaseDocument();
    converter.write(entity, converted);
    assertEquals(created.getTime(), converted.getPayload().get("created"));
    assertEquals(modified.getTimeInMillis() / 1000, converted.getPayload().get("modified"));
    assertEquals(deleted.toDate().getTime(), converted.getPayload().get("deleted"));

    DateEntity read = converter.read(DateEntity.class, converted);
    assertEquals(created.getTime(), read.created.getTime());
    assertEquals(modified.getTimeInMillis() / 1000, read.modified.getTimeInMillis() / 1000);
    assertEquals(deleted.toDate().getTime(), read.deleted.toDate().getTime());
  }

  static class EntityWithoutID {
    private String attr0;
    public EntityWithoutID(String a0) {
      attr0 = a0;
    }
  }

  static class BaseEntity {
    public static final String ID = "mockid";
    @Id
    private String id = ID;
  }

  static class StringEntity extends BaseEntity {
    private String attr0;
    public StringEntity(String attr0) {
      this.attr0 = attr0;
    }
  }

  static class NumberEntity extends BaseEntity {
    private long attr0;
    public NumberEntity(long attr0) {
      this.attr0 = attr0;
    }
  }

  static class BooleanEntity extends BaseEntity {
    private boolean attr0;
    public BooleanEntity(boolean attr0) {
      this.attr0 = attr0;
    }
  }

  static class MixedSimpleEntity extends BaseEntity {
    private String attr0;
    private int attr1;
    private double attr2;
    private boolean attr3;
    public MixedSimpleEntity(String attr0, int attr1, double attr2, boolean attr3) {
      this.attr0 = attr0;
      this.attr1 = attr1;
      this.attr2 = attr2;
      this.attr3 = attr3;
    }
  }

  static class UninitializedEntity extends BaseEntity {
    private String attr0 = null;
    private int attr1;
    private Integer attr2;
  }

  static class MapEntity extends BaseEntity {
    private Map<String, String> attr0;
    private Map<String, Boolean> attr1;
    private Map<Integer, String> attr2;
    private Map<String, Map<String, String>> attr3;
    public MapEntity(Map<String, String> attr0, Map<String, Boolean> attr1, Map<Integer, String> attr2, Map<String, Map<String, String>> attr3) {
      this.attr0 = attr0;
      this.attr1 = attr1;
      this.attr2 = attr2;
      this.attr3 = attr3;
    }
  }

  static class ListEntity extends BaseEntity {
    private List<String> attr0;
    private List<Integer> attr1;
    private List<List<String>> attr2;
    ListEntity(List<String> attr0, List<Integer> attr1, List<List<String>> attr2) {
      this.attr0 = attr0;
      this.attr1 = attr1;
      this.attr2 = attr2;
    }
  }

  static class SetEntity extends BaseEntity {
    private Set<String> attr0;
    private Set<Integer> attr1;
    private Set<Set<String>> attr2;
    SetEntity(Set<String> attr0, Set<Integer> attr1, Set<Set<String>> attr2) {
      this.attr0 = attr0;
      this.attr1 = attr1;
      this.attr2 = attr2;
    }
  }

  static class ValueEntity extends BaseEntity {
    private Email email;
    private List<Email> listOfEmails;

    public ValueEntity(Email email, List<Email> listOfEmails) {
      this.email = email;
      this.listOfEmails = listOfEmails;
    }
  }

  static class Email {
    private String emailAddr;
    public Email(String emailAddr) {
      this.emailAddr = emailAddr;
    }
  }

  static class CustomEntity extends BaseEntity {
    private BigDecimal value;
    private List<BigDecimal> listOfValues;
    private Map<String, BigDecimal> mapOfValues;

    public CustomEntity(BigDecimal value, List<BigDecimal> listOfValues, Map<String, BigDecimal> mapOfValues) {
      this.value = value;
      this.listOfValues = listOfValues;
      this.mapOfValues = mapOfValues;
    }
  }

  static class CustomObjectEntity extends BaseEntity {
    private CustomObject object;
    private List<CustomObject> listOfObjects;
    private Map<String, CustomObject> mapOfObjects;

    public CustomObjectEntity(CustomObject object, List<CustomObject> listOfObjects, Map<String, CustomObject> mapOfObjects) {
      this.object = object;
      this.listOfObjects = listOfObjects;
      this.mapOfObjects = mapOfObjects;
    }
  }

  static class CustomObject {
    private BigDecimal weight;
    public CustomObject(BigDecimal weight) {
      this.weight = weight;
    }
  }

  @WritingConverter
  public static enum BigDecimalToStringConverter implements Converter<BigDecimal, String> {
    INSTANCE;

    @Override
    public String convert(BigDecimal source) {
      return source.toPlainString();
    }
  }

  @ReadingConverter
  public static enum StringToBigDecimalConverter implements Converter<String, BigDecimal> {
    INSTANCE;

    @Override
    public BigDecimal convert(String source) {
      return new BigDecimal(source);
    }
  }

  static class DateEntity extends BaseEntity {
    private Date created;
    private Calendar modified;
    private LocalDateTime deleted;

    public DateEntity(Date created, Calendar modified, LocalDateTime deleted) {
      this.created = created;
      this.modified = modified;
      this.deleted = deleted;
    }
  }

}
