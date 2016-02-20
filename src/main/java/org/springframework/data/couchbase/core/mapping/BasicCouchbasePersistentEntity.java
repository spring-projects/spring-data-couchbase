/*
 * Copyright 2012-2015 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.couchbase.core.mapping;

import com.couchbase.client.java.repository.annotation.Id;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.expression.BeanFactoryAccessor;
import org.springframework.context.expression.BeanFactoryResolver;
import org.springframework.data.mapping.model.BasicPersistentEntity;
import org.springframework.data.util.TypeInformation;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import java.util.Calendar;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

/**
 * The representation of a persistent entity.
 *
 * @author Michael Nitschinger
 */
public class BasicCouchbasePersistentEntity<T> extends BasicPersistentEntity<T, CouchbasePersistentProperty>
    implements CouchbasePersistentEntity<T>, ApplicationContextAware {

  /**
   * Contains the evaluation context.
   */
  private final StandardEvaluationContext context;

  /**
   * Create a new entity.
   *
   * @param typeInformation the type information of the entity.
   */
  public BasicCouchbasePersistentEntity(final TypeInformation<T> typeInformation) {
    super(typeInformation);
    context = new StandardEvaluationContext();
  }

  /**
   * Sets the application context.
   *
   * @param applicationContext the application context.
   * @throws BeansException if setting the application context did go wrong.
   */
  @Override
  public void setApplicationContext(final ApplicationContext applicationContext) throws BeansException {
    context.addPropertyAccessor(new BeanFactoryAccessor());
    context.setBeanResolver(new BeanFactoryResolver(applicationContext));
    context.setRootObject(applicationContext);
  }

  // DATACOUCH-145: allows SDK's @Id annotation to be used
  @Override
  protected CouchbasePersistentProperty returnPropertyIfBetterIdPropertyCandidateOrNull(CouchbasePersistentProperty property) {
    if (!property.isIdProperty()) {
      return null;
    }

    if (!this.hasIdProperty()) {
      return property;
    }

    //check existing ID vs new candidate
    boolean currentCbId = this.getIdProperty().isAnnotationPresent(Id.class);
    boolean currentSpringId = this.getIdProperty().isAnnotationPresent(org.springframework.data.annotation.Id.class);
    boolean candidateCbId = property.isAnnotationPresent(Id.class);
    boolean candidateSpringId = property.isAnnotationPresent(org.springframework.data.annotation.Id.class);

    if (currentCbId && candidateSpringId) {
      //spring IDs will have priority over SDK IDs
      return property;
    } else if (currentSpringId && candidateCbId) {
      //ignore SDK's IDs if current is a Spring ID
      return null;
    }
    /* any of the following will throw:
      - current is a spring ID and the candidate bears another spring ID
      - current is a SDK ID and the candidate bears another SDK ID
      - any other combination involving something else than a SDK or Spring ID
     */
    return super.returnPropertyIfBetterIdPropertyCandidateOrNull(property);
  }

  @Override
  public int getExpiry() {
    org.springframework.data.couchbase.core.mapping.Document annotation =
        getType().getAnnotation(org.springframework.data.couchbase.core.mapping.Document.class);
    if (annotation == null)
      return 0;

    long secondsShift = annotation.expiryUnit().toSeconds(annotation.expiry());
    if (secondsShift > TTL_IN_SECONDS_INCLUSIVE_END) {
      //we want it to be represented as a UNIX timestamp style, seconds since Epoch in UTC
      Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
      if (annotation.expiryUnit() == TimeUnit.DAYS) {
        //makes sure we won't lose resolution
        cal.add(Calendar.DAY_OF_MONTH, annotation.expiry());
      } else {
        //use the shift in seconds since resolution should be smaller
        cal.add(Calendar.SECOND, (int) secondsShift);
      }
      return (int) (cal.getTimeInMillis() / 1000); //note: Unix UTC time representation in int is okay until year 2038
    } else {
      return (int) secondsShift;
    }
  }

  @Override
  public boolean isTouchOnRead() {
    org.springframework.data.couchbase.core.mapping.Document annotation = getType().getAnnotation(
        org.springframework.data.couchbase.core.mapping.Document.class);
    return annotation == null ? false : getExpiry() > 0 && annotation.touchOnRead();
  }

}
