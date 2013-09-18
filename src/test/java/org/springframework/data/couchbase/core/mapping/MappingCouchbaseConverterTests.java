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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.annotation.Id;
import org.springframework.data.couchbase.TestApplicationConfig;
import org.springframework.data.couchbase.core.convert.MappingCouchbaseConverter;
import org.springframework.data.mapping.model.MappingException;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.*;

import static org.junit.Assert.*;

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
  public void writesString() {
    CouchbaseDocument converted = new CouchbaseDocument();
    StringEntity entity = new StringEntity("foobar");

    Map<String, Object> expected = new HashMap<String, Object>();
    expected.put("_class", entity.getClass().getName());
    expected.put("attr0", "foobar");

    converter.write(entity, converted);
    assertEquals(expected.toString(), converted.export().toString());
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

    Map<String, Object> expected = new HashMap<String, Object>();
    expected.put("_class", entity.getClass().getName());
    expected.put("attr0", 42);

    converter.write(entity, converted);
    assertEquals(expected.toString(), converted.export().toString());
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

    Map<String, Object> expected = new HashMap<String, Object>();
    expected.put("_class", entity.getClass().getName());
    expected.put("attr0", true);

    converter.write(entity, converted);
    assertEquals(expected.toString(), converted.export().toString());
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

    Map<String, Object> expected = new HashMap<String, Object>();
    expected.put("_class", entity.getClass().getName());
    expected.put("attr0", "a");
    expected.put("attr1", 5);
    expected.put("attr2", -0.3);
    expected.put("attr3", true);

    converter.write(entity, converted);
    assertEquals(expected.toString(), converted.export().toString());
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
    // given:
    CouchbaseDocument document = new CouchbaseDocument("001");

    // when:
    BasicCouchbasePersistentPropertyTests.Beer beer = converter.read(BasicCouchbasePersistentPropertyTests.Beer.class,
        document);

    // then:
    assertEquals("001", beer.getId());
  }

  @Test
  public void writesUninitializedValues() {
    CouchbaseDocument converted = new CouchbaseDocument();
    UninitializedEntity entity = new UninitializedEntity();

    Map<String, Object> expected = new HashMap<String, Object>();
    expected.put("_class", entity.getClass().getName());
    expected.put("attr1", 0);

    converter.write(entity, converted);
    assertEquals(expected.toString(), converted.export().toString());
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

    Map<String, Object> expected = new HashMap<String, Object>();
    expected.put("_class", entity.getClass().getName());
    expected.put("attr0", attr0);
    expected.put("attr1", attr1);
    expected.put("attr2", attr2);
    expected.put("attr3", attr3);

    converter.write(entity, converted);
    assertEquals(expected.toString(), converted.export().toString());


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

    Map<String, Object> expected = new HashMap<String, Object>();
    expected.put("_class", entity.getClass().getName());
    expected.put("attr0", attr0);
    expected.put("attr1", attr1);
    expected.put("attr2", attr2);

    converter.write(entity, converted);
    assertEquals(expected.toString(), converted.export().toString());

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
    System.out.println(readConverted.attr0);
    System.out.println(readConverted.attr1);
    System.out.println(readConverted.attr2);
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
    Map<String, Object> expected = new HashMap<String, Object>();
    expected.put("_class", entity.getClass().getName());
    expected.put("attr0", attr0);
    expected.put("attr1", attr1);
    expected.put("attr2", attr2);

    converter.write(entity, converted);
    assertEquals(expected.toString(), converted.export().toString());

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

    Map<String, Object> expected = new HashMap<String, Object>();
    expected.put("_class", entity.getClass().getName());
    expected.put("email", new HashMap<String, Object>() {{
      put("emailAddr", email);
    }});
    expected.put("listOfEmails", new ArrayList<Object>() {{
      add(new HashMap<String, Object>() {{
        put("emailAddr", email);
      }});
    }});

    assertEquals(expected.toString(), converted.export().toString());

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
    public MapEntity(Map attr0, Map attr1, Map attr2, Map attr3) {
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

}
