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

package org.springframework.data.couchbase.core.mapping.event;

import org.springframework.data.couchbase.core.mapping.CouchbaseDocument;

import java.util.ArrayList;

/**
 * @author Michael Nitschinger
 */
public class SimpleMappingEventListener extends AbstractCouchbaseEventListener<Object> {

  public final ArrayList<BeforeConvertEvent<Object>> onBeforeConvertEvents = new ArrayList<BeforeConvertEvent<Object>>();
  public final ArrayList<BeforeSaveEvent<Object>> onBeforeSaveEvents = new ArrayList<BeforeSaveEvent<Object>>();
  public final ArrayList<AfterSaveEvent<Object>> onAfterSaveEvents = new ArrayList<AfterSaveEvent<Object>>();

  @Override
  public void onBeforeConvert(Object source) {
    onBeforeConvertEvents.add(new BeforeConvertEvent<Object>(source));
  }

  @Override
  public void onBeforeSave(Object source, CouchbaseDocument doc) {
    onBeforeSaveEvents.add(new BeforeSaveEvent<Object>(source, doc));
  }

  @Override
  public void onAfterSave(Object source, CouchbaseDocument doc) {
    onAfterSaveEvents.add(new AfterSaveEvent<Object>(source, doc));
  }

}
