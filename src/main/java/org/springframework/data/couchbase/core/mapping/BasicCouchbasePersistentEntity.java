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

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.expression.BeanFactoryAccessor;
import org.springframework.context.expression.BeanFactoryResolver;
import org.springframework.data.mapping.model.BasicPersistentEntity;
import org.springframework.data.util.TypeInformation;
import org.springframework.expression.spel.support.StandardEvaluationContext;

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

  /**
   * Returns the expiration time of the entity.
   *
   * @return the expiration time.
   */
  public int getExpiry() {
  	org.springframework.data.couchbase.core.mapping.Document annotation = 
  			getType().getAnnotation(org.springframework.data.couchbase.core.mapping.Document.class);
    return annotation == null ? 0 : annotation.expiry();
  }
  
}
