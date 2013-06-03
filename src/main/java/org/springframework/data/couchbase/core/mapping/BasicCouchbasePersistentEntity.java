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
 * @author Michael Nitschinger
 */
public class BasicCouchbasePersistentEntity<T>
  extends BasicPersistentEntity<T, CouchbasePersistentProperty>
  implements CouchbasePersistentEntity<T>, ApplicationContextAware {

  private final StandardEvaluationContext context;

  public BasicCouchbasePersistentEntity(TypeInformation<T> typeInformation) {
    super(typeInformation);

    this.context = new StandardEvaluationContext();
  }

  @Override
  public void setApplicationContext(ApplicationContext applicationContext)
    throws BeansException {
		context.addPropertyAccessor(new BeanFactoryAccessor());
		context.setBeanResolver(new BeanFactoryResolver(applicationContext));
		context.setRootObject(applicationContext);
  }
  
  public int getExpiry() {
  	org.springframework.data.couchbase.core.mapping.Document annotation = 
  			getType().getAnnotation(org.springframework.data.couchbase.core.mapping.Document.class);
  	
  	if(annotation == null) {
  		return 0;
  	}
  	return annotation.expiry();
  }
  
}
