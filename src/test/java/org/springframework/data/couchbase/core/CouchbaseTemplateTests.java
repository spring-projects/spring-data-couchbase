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

package org.springframework.data.couchbase.core;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Version;
import org.springframework.data.couchbase.TestApplicationConfig;
import org.springframework.data.couchbase.core.mapping.Document;
import org.springframework.data.couchbase.core.mapping.Field;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.StringDocument;
import com.couchbase.client.java.error.DocumentDoesNotExistException;
import com.couchbase.client.java.view.Stale;
import com.couchbase.client.java.view.ViewQuery;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author Michael Nitschinger
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = TestApplicationConfig.class)
@TestExecutionListeners(CouchbaseTemplateViewListener.class)
public class CouchbaseTemplateTests {

  /**
   * JSON object mapper to verify results.
   */
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Autowired
  private Bucket client;

  @Autowired
  private CouchbaseTemplate template;

  @Test
  public void saveSimpleEntityCorrectly() throws Exception {
    StringDocument id = StringDocument.create("beers:awesome-stout");
    String name = "The Awesome Stout";
    boolean active = false;
    Beer beer = new Beer(id.id()).setName(name).setActive(active);

    template.save(beer);
    StringDocument result = client.get(id);

    assertNotNull(result);
    Map<String, Object> converted =
        MAPPER.readValue(result.content().toString(), new TypeReference<Map<String, Object>>() {});
    assertEquals("org.springframework.data.couchbase.core.Beer", converted.get("_class"));
    assertEquals(false, converted.get("is_active"));
    assertEquals("The Awesome Stout", converted.get("name"));
  }

  @Test
  public void saveDocumentWithExpiry() throws Exception {
    String id = "simple-doc-with-expiry";
    DocumentWithExpiry doc = new DocumentWithExpiry(id);
    template.save(doc);
    StringDocument document = StringDocument.create(id);
    assertNotNull(client.get(document));
    Thread.sleep(3000);
    assertNull(client.get(document));
  }

  @Test
  public void insertDoesNotOverride() throws Exception {
    String id = "double-insert-test";
    StringDocument documentId = StringDocument.create(id);
    client.remove(documentId);

    SimplePerson doc = new SimplePerson(id, "Mr. A");
    template.insert(doc);
    StringDocument result = client.get(documentId);
    Map<String, Object> converted =
        MAPPER.readValue(result.content().toString(), new TypeReference<Map<String, Object>>() {});
    assertEquals("org.springframework.data.couchbase.core.CouchbaseTemplateTests$SimplePerson", converted.get("_class"));
    assertEquals("Mr. A", converted.get("name"));

    doc = new SimplePerson(id, "Mr. B");
    template.insert(doc);
    result = client.get(documentId);

    converted =
        MAPPER.readValue(result.content().toString(), new TypeReference<Map<String, Object>>() {});
    assertEquals("org.springframework.data.couchbase.core.CouchbaseTemplateTests$SimplePerson", converted.get("_class"));
    assertEquals("Mr. A", converted.get("name"));
  }


  @Test(expected = DocumentDoesNotExistException.class)
  public void updateDoesNotInsert() {
    StringDocument id = StringDocument.create("update-does-not-insert");
    SimplePerson doc = new SimplePerson(id.id(), "Nice Guy");
    template.update(doc);
    assertNull(client.get(id));
  }


  @Test
  public void removeDocument() {
    String id = "beers:to-delete-stout";
    StringDocument documentId = StringDocument.create(id);
    Beer beer = new Beer(id);

    template.save(beer);
    Object result = client.get(documentId);
    assertNotNull(result);

    template.remove(beer);
    result = client.get(documentId);
    assertNull(result);
  }


  @Test
  public void storeListsAndMaps() {
    StringDocument id = StringDocument.create("persons:lots-of-names");
    List<String> names = new ArrayList<String>();
    names.add("Michael");
    names.add("Thomas");
    names.add(null);
    List<Integer> votes = new LinkedList<Integer>();
    Map<String, Boolean> info1 = new HashMap<String, Boolean>();
    info1.put("foo", true);
    info1.put("bar", false);
    info1.put("nullValue", null);
    Map<String, Integer> info2 = new HashMap<String, Integer>();

    ComplexPerson complex = new ComplexPerson(id.id(), names, votes, info1, info2);

    template.save(complex);

    ComplexPerson response = template.findById(id.id(), ComplexPerson.class);
    assertEquals(names, response.getFirstnames());
    assertEquals(votes, response.getVotes());
    assertEquals(id.id(), response.getId());
    assertEquals(info1, response.getInfo1());
    assertEquals(info2, response.getInfo2());
  }


  @Test
  public void validFindById() {
    StringDocument id = StringDocument.create("beers:findme-stout");
    String name = "The Findme Stout";
    boolean active = true;
    Beer beer = new Beer(id.id()).setName(name).setActive(active);
    template.save(beer);

    Beer found = template.findById(id.id(), Beer.class);

    assertNotNull(found);
    assertEquals(id.id(), found.getId());
    assertEquals(name, found.getName());
    assertEquals(active, found.getActive());
  }

  @Test
  public void shouldLoadAndMapViewDocs() {
    ViewQuery query = ViewQuery.from("test_beers", "by_name").reduce(false).stale(Stale.FALSE);

    final List<Beer> beers = template.findByView(query, Beer.class);
    assertTrue(beers.size() > 0);

    for (Beer beer : beers) {
      assertNotNull(beer.getId());
      assertNotNull(beer.getName());
      assertNotNull(beer.getActive());
    }
  }

  @Test
  public void shouldDeserialiseLongs() {
    final long time = new Date().getTime();
    SimpleWithLong simpleWithLong = new SimpleWithLong("simpleWithLong:simple", time);
    template.save(simpleWithLong);
    simpleWithLong = template.findById("simpleWithLong:simple", SimpleWithLong.class);
    assertNotNull(simpleWithLong);
    assertEquals(time, simpleWithLong.getValue());
  }

  @Test
  public void shouldDeserialiseEnums() {
    SimpleWithEnum simpleWithEnum = new SimpleWithEnum("simpleWithEnum:enum", SimpleWithEnum.Type.BIG);
    template.save(simpleWithEnum);
    simpleWithEnum = template.findById("simpleWithEnum:enum", SimpleWithEnum.class);
    assertNotNull(simpleWithEnum);
    assertEquals(simpleWithEnum.getType(), SimpleWithEnum.Type.BIG);
  }

  @Test
  public void shouldDeserialiseClass() {
    SimpleWithClass simpleWithClass = new SimpleWithClass("simpleWithClass:class", Integer.class);
    simpleWithClass.setValue("The dish ran away with the spoon.");
    template.save(simpleWithClass);
    simpleWithClass = template.findById("simpleWithClass:class", SimpleWithClass.class);
    assertNotNull(simpleWithClass);
    assertThat(simpleWithClass.getValue(), equalTo("The dish ran away with the spoon."));
  }

  @Test
  public void shouldHandleCASVersionOnInsert() throws Exception {
    client.remove("versionedClass:1");

    VersionedClass versionedClass = new VersionedClass("versionedClass:1", "foobar");
    assertEquals(0, versionedClass.getVersion());
    template.insert(versionedClass);
    JsonDocument rawStored = client.get("versionedClass:1");
    assertEquals(rawStored.cas(), versionedClass.getVersion());
  }

  @Test
  public void versionShouldNotUpdateOnSecondInsert() throws Exception {
    client.remove("versionedClass:2");

    VersionedClass versionedClass = new VersionedClass("versionedClass:2", "foobar");
    template.insert(versionedClass);
    long version1 = versionedClass.getVersion();
    template.insert(versionedClass);
    long version2 = versionedClass.getVersion();

    assertTrue(version1 > 0);
    assertTrue(version2 > 0);
    assertEquals(version1, version2);
  }

  @Test
  public void shouldSaveDocumentOnMatchingVersion() throws Exception {
    client.remove("versionedClass:3");

    VersionedClass versionedClass = new VersionedClass("versionedClass:3", "foobar");
    template.insert(versionedClass);
    long version1 = versionedClass.getVersion();

    versionedClass.setField("foobar2");
    template.save(versionedClass);
    long version2 = versionedClass.getVersion();

    assertTrue(version1 > 0);
    assertTrue(version2 > 0);
    assertNotEquals(version1, version2);

    assertEquals("foobar2", template.findById("versionedClass:3", VersionedClass.class).getField());
  }

  @Test(expected = OptimisticLockingFailureException.class)
  public void shouldNotSaveDocumentOnNotMatchingVersion() throws Exception {
    StringDocument id = StringDocument.create("versionedClass:6");
    client.remove(id);

    VersionedClass versionedClass = new VersionedClass("versionedClass:6", "foobar");
    template.insert(versionedClass);

    final StringDocument newOne = StringDocument.create("versionedClass:6", "{}");
    final StringDocument upsert = client.upsert(newOne);
    assertTrue(upsert.cas() > 0);

    versionedClass.setField("foobar2");
    template.save(versionedClass);
  }

  @Test
  public void shouldUpdateDocumentOnMatchingVersion() throws Exception {
    client.remove("versionedClass:5");

    VersionedClass versionedClass = new VersionedClass("versionedClass:5", "foobar");
    template.insert(versionedClass);
    long version1 = versionedClass.getVersion();

    versionedClass.setField("foobar2");
    template.update(versionedClass);
    long version2 = versionedClass.getVersion();

    assertTrue(version1 > 0);
    assertTrue(version2 > 0);
    assertNotEquals(version1, version2);

    assertEquals("foobar2", template.findById("versionedClass:5", VersionedClass.class).getField());
  }

  @Test(expected = OptimisticLockingFailureException.class)
  public void shouldNotUpdateDocumentOnNotMatchingVersion() throws Exception {
    StringDocument id = StringDocument.create("versionedClass:6");
    client.remove(id);

    VersionedClass versionedClass = new VersionedClass("versionedClass:6", "foobar");
    template.insert(versionedClass);

    final StringDocument newOne = StringDocument.create("versionedClass:6", "{}");
    final StringDocument upsert = client.upsert(newOne);
    assertTrue(upsert.cas() > 0);

    versionedClass.setField("foobar2");
    template.update(versionedClass);
  }

  @Test
  public void shouldLoadVersionPropertyOnFind() throws Exception {
    client.remove("versionedClass:7");

    VersionedClass versionedClass = new VersionedClass("versionedClass:7", "foobar");
    template.insert(versionedClass);
    assertTrue(versionedClass.getVersion() > 0);

    VersionedClass foundClass = template.findById("versionedClass:7", VersionedClass.class);
    assertEquals(versionedClass.getVersion(), foundClass.getVersion());
  }

  /**
   * A sample document with just an id and property.
   */
  @Document
  static class SimplePerson {

    @Id
    private final String id;
    @Field
    private final String name;

    public SimplePerson(String id, String name) {
      this.id = id;
      this.name = name;
    }
  }

  /**
   * A sample document that expires in 2 seconds.
   */
  @Document(expiry = 2)
  static class DocumentWithExpiry {

    @Id
    private final String id;

    public DocumentWithExpiry(String id) {
      this.id = id;
    }
  }

  @Document
  static class ComplexPerson {

    @Id
    private final String id;
    @Field
    private final List<String> firstnames;
    @Field
    private final List<Integer> votes;

    @Field
    private final Map<String, Boolean> info1;
    @Field
    private final Map<String, Integer> info2;

    public ComplexPerson(String id, List<String> firstnames,
                         List<Integer> votes, Map<String, Boolean> info1,
                         Map<String, Integer> info2) {
      this.id = id;
      this.firstnames = firstnames;
      this.votes = votes;
      this.info1 = info1;
      this.info2 = info2;
    }

    List<String> getFirstnames() {
      return firstnames;
    }

    List<Integer> getVotes() {
      return votes;
    }

    Map<String, Boolean> getInfo1() {
      return info1;
    }

    Map<String, Integer> getInfo2() {
      return info2;
    }

    String getId() {
      return id;
    }
  }

  @Document
  static class SimpleWithLong {

    @Id
    private String id;

    private long value;

    SimpleWithLong(final String id, final long value) {
      this.id = id;
      this.value = value;
    }

    String getId() {
      return id;
    }

    long getValue() {
      return value;
    }

    void setValue(final long value) {
      this.value = value;
    }
  }

  static class SimpleWithEnum {

    @Id
    private String id;

    private enum Type {
      BIG
    }

    private Type type;

    SimpleWithEnum(final String id, final Type type) {
      this.id = id;
      this.type = type;
    }

    String getId() {
      return id;
    }

    void setId(final String id) {
      this.id = id;
    }

    Type getType() {
      return type;
    }

    void setType(final Type type) {
      this.type = type;
    }
  }

  static class SimpleWithClass {

    @Id
    private String id;

    private Class<Integer> integerClass;

    private String value;

    SimpleWithClass(final String id, final Class<Integer> integerClass) {
      this.id = id;
      this.integerClass = integerClass;
    }

    String getId() {
      return id;
    }

    void setId(final String id) {
      this.id = id;
    }

    Class<Integer> getIntegerClass() {
      return integerClass;
    }

    void setIntegerClass(final Class<Integer> integerClass) {
      this.integerClass = integerClass;
    }

    String getValue() { return value; }

    void setValue(final String value) {
      this.value = value;
    }
  }

  static class VersionedClass {

    @Id
    private String id;

    @Version
    private long version;

    private String field;

    VersionedClass(String id, String field) {
      this.id = id;
      this.field = field;
    }

    public String getId() {
      return id;
    }

    public long getVersion() {
      return version;
    }

    public String getField() {
      return field;
    }

    public void setField(String field) {
      this.field = field;
    }
  }

}
