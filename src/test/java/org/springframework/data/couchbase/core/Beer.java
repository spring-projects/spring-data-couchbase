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

import org.springframework.data.annotation.Id;
import org.springframework.data.couchbase.core.mapping.Field;


/**
 * Test class for persisting and loading from {@link CouchbaseTemplate}.
 *
 * @author Michael Nitschinger
 */
public class Beer {

  @Id
  private final String id;

  private String name;

  @Field("is_active")
  private boolean active = true;

  public Beer(String id) {
    this.id = id;
  }

  @Override
  public String toString() {
    return "Beer [id=" + id + ", name=" + name + ", active=" + active + "]";
  }

  public Beer setName(String name) {
    this.name = name;
    return this;
  }

  public String getName() {
    return name;
  }

  public Beer setActive(boolean active) {
    this.active = active;
    return this;
  }

  public boolean getActive() {
    return active;
  }
  
  public String getId() {
  	return id;
  }

}
