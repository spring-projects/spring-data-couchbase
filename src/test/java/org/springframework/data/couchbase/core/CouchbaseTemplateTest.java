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

import com.couchbase.client.CouchbaseClient;

import static org.junit.Assert.*;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.annotation.Id;
import org.springframework.data.couchbase.TestApplicationConfig;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.core.mapping.Document;
import org.springframework.data.couchbase.core.mapping.Field;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.net.SocketAddress;
import java.util.*;

/**
 * @author Michael Nitschinger
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = TestApplicationConfig.class)
public class CouchbaseTemplateTest {

  @Autowired
  private CouchbaseClient client;

  @Autowired
  private CouchbaseTemplate template;

  @Test
  public void saveSimpleEntityCorrectly() throws Exception {
    String id = "beers:awesome-stout";
    String name = "The Awesome Stout";
    boolean active = false;
    Beer beer = new Beer(id).setName(name).setActive(active);

    template.save(beer);
    String result = (String) client.get(id);

    String expected = "{\"is_active\":" + active + ",\"name\":\"" + name + "\"}";
    assertNotNull(result);
    assertEquals(expected, result);
  }
  
  @Test
  public void saveDocumentWithExpiry() throws Exception {
  	String id = "simple-doc-with-expiry";
  	DocumentWithExpiry doc = new DocumentWithExpiry(id);
  	template.save(doc);
  	assertNotNull(client.get(id));
  	Thread.sleep(3000);
  	assertNull(client.get(id));
  }
  
  @Test
  public void insertDoesNotOverride() {
  	String id ="double-insert-test";
  	String expected = "{\"name\":\"Mr. A\"}";

  	SimplePerson doc = new SimplePerson(id, "Mr. A");
  	template.insert(doc);
  	String result = (String) client.get(id);
  	assertEquals(expected, result);
  	
  	doc = new SimplePerson(id, "Mr. B");
  	template.insert(doc);
  	result = (String) client.get(id);
  	assertEquals(expected, result);
  }
  
  @Test
  public void updateDoesNotInsert() {
  	String id ="update-does-not-insert";
  	SimplePerson doc = new SimplePerson(id, "Nice Guy");
  	template.update(doc);
  	assertNull(client.get(id));
  }
  
  @Test
  public void validFindById() {
    String id = "beers:findme-stout";
    String name = "The Findme Stout";
    boolean active = true;
    Beer beer = new Beer(id).setName(name).setActive(active);
  	template.save(beer);
  	
  	Beer found = template.findById(id, Beer.class);
  	assertNotNull(found);
  	assertEquals(id, found.getId());
  	assertEquals(name, found.getName());
  	assertEquals(active, found.getActive());
  }

  @Test
  public void removeDocument() {
    String id = "beers:findme-stout";
    Object result = client.get(id);
    assertNotNull(result);

    Beer beer = new Beer(id);
    template.remove(beer);

    result = client.get(id);
    assertNull(result);
  }

  @Test
  public void storeListsAndMaps() {
    String id ="persons:lots-of-names";
    List<String> names = new ArrayList<String>();
    names.add("Michael");
    names.add("Thomas");
    List<Integer> votes = new LinkedList<Integer>();
    Map<String, Boolean> info1 = new HashMap<String, Boolean>();
    info1.put("foo", true);
    info1.put("bar", false);
    Map<String, Integer> info2 = new HashMap<String, Integer>();

    ComplexPerson complex = new ComplexPerson(id, names, votes, info1, info2);

    template.save(complex);

    String expected = "{\"info1\":{\"foo\":true,\"bar\":false},\"votes\":[],"
      + "\"firstnames\":[\"Michael\",\"Thomas\"],\"info2\":{}}";
    assertEquals(expected, client.get(id));

    ComplexPerson response = template.findById(id, ComplexPerson.class);
    assertEquals(names, response.getFirstnames());
    assertEquals(votes, response.getVotes());
    assertEquals(id, response.getId());
    assertEquals(info1, response.getInfo1());
    assertEquals(info2, response.getInfo2());
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
  @Document(expiry=2)
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
}
