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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.junit.Assert.assertEquals;

/**
 * @author Michael Nitschinger
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = EventContextConfiguration.class)
public class AbstractCouchbaseEventListenerTests {

  @Autowired
  private CouchbaseTemplate couchbaseTemplate;

  @Autowired
  private SimpleMappingEventListener eventListener;

  @Test
  public void shouldEmitEvents() {
    assertEquals(0, eventListener.onBeforeSaveEvents.size());
    assertEquals(0, eventListener.onAfterSaveEvents.size());
    assertEquals(0, eventListener.onBeforeConvertEvents.size());

    couchbaseTemplate.save(new User("john smith", 18));

    assertEquals(1, eventListener.onBeforeSaveEvents.size());
    assertEquals(1, eventListener.onAfterSaveEvents.size());
    assertEquals(1, eventListener.onBeforeConvertEvents.size());
  }

}
