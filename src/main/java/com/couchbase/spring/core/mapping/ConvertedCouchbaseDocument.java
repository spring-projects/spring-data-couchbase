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

package com.couchbase.spring.core.mapping;

import java.util.HashMap;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.springframework.data.mapping.model.MappingException;

public class ConvertedCouchbaseDocument {

  private String id;

  private String rawValue;

  private int expiry;
  
  private Map<String, Object> decoded;

  public ConvertedCouchbaseDocument() {
    this("", "", 0);
  }

  public ConvertedCouchbaseDocument(String id, String rawValue) {
    this(id, rawValue, 0);
  }

  public ConvertedCouchbaseDocument(String id, String rawValue, int expiry) {
    this.id = id;
    this.rawValue = rawValue;
    this.expiry = expiry;
    this.decoded = new HashMap<String, Object>();
    parseJson();
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getId() {
    return id;
  }

  public String getRawValue() {
    return rawValue;
  }

  public void setRawValue(String value) {
    this.rawValue = value;
    parseJson();
    
  }

  public int getExpiry() {
    return expiry;
  }

  public void setExpiry(int expiry) {
    this.expiry = expiry;
  }
  
  public boolean containsField(String fieldname) {
  	return decoded.containsKey(fieldname);
  }
  
  public Object get(String fieldname) {
  	return decoded.get(fieldname);
  }
  
  private void parseJson() {
  	ObjectMapper mapper = new ObjectMapper();
  	try {
  		if(!getRawValue().isEmpty()) {
  			Map<String, Object> converted = mapper.readValue(getRawValue(), 
  				new TypeReference<Map<String, Object>>() { });
  			this.decoded = converted;
  		}
  	} catch(Exception e) {
  		throw new MappingException("Error while decoding JSON object.", e);
  	}
  }

}
