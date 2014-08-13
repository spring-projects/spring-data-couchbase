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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationListener;
import org.springframework.data.couchbase.core.mapping.CouchbaseDocument;

/**
 * A{@link ApplicationListener} for Couchbase mapping events logging the events.
 *
 * @author Michael Nitschinger
 */
public class LoggingEventListener extends AbstractCouchbaseEventListener<Object> {

  private static final Logger LOGGER = LoggerFactory.getLogger(LoggingEventListener.class);

  @Override
  public void onBeforeDelete(Object source, CouchbaseDocument doc) { LOGGER.info("onBeforeDelete: {}, {}", source, doc); }

  @Override
  public void onAfterDelete(Object source, CouchbaseDocument doc) { LOGGER.info("onAfterDelete: {} {}", source, doc); }

  @Override
  public void onAfterSave(Object source, CouchbaseDocument doc) {
    LOGGER.info("onAfterSave: {}, {}", source, doc);
  }

  @Override
  public void onBeforeSave(Object source, CouchbaseDocument doc) {
    LOGGER.info("onBeforeSave: {}, {}", source, doc);
  }

  @Override
  public void onBeforeConvert(Object source) {
    LOGGER.info("onBeforeConvert: {}, {}", source);
  }

}
