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

import java.util.HashMap;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.springframework.data.mapping.model.MappingException;

/**
 * @author Michael Nitschinger
 */
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
