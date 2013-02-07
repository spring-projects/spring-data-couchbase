/**
 * Copyright (C) 2009-2012 Couchbase, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALING
 * IN THE SOFTWARE.
 */

package com.couchbase.spring.core;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.spring.TestApplicationConfig;
import com.couchbase.spring.core.mapping.Document;
import com.couchbase.spring.core.mapping.Field;

import static org.junit.Assert.*;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.annotation.Id;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

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
  
  /**
   * A sample document with just an id and property.
   */
  @Document
  class SimplePerson {
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
  class DocumentWithExpiry {
    @Id
    private final String id;
    
    public DocumentWithExpiry(String id) {
    	this.id = id;
    }
  }
}
