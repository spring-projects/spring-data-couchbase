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

import com.couchbase.spring.core.mapping.BasicCouchbasePersistentEntity;
import com.couchbase.spring.core.mapping.BasicCouchbasePersistentProperty;
import com.couchbase.spring.core.mapping.CouchbasePersistentProperty;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.data.mapping.context.AbstractMappingContext;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.data.util.TypeInformation;

public class CouchbaseMappingContext
  extends AbstractMappingContext<BasicCouchbasePersistentEntity<?>, CouchbasePersistentProperty>
  implements ApplicationContextAware {

  private ApplicationContext context;

  @Override
  protected <T> BasicCouchbasePersistentEntity<?> createPersistentEntity(TypeInformation<T> typeInformation) {
    BasicCouchbasePersistentEntity<T> entity = new BasicCouchbasePersistentEntity<T>(typeInformation);
    if(context != null) {
      entity.setApplicationContext(context);
    }
    return entity;
  }

  @Override
  protected CouchbasePersistentProperty createPersistentProperty(Field field, PropertyDescriptor descriptor, BasicCouchbasePersistentEntity<?> owner, SimpleTypeHolder simpleTypeHolder) {
    return new BasicCouchbasePersistentProperty(field, descriptor, owner, simpleTypeHolder);
  }

	@Override
	public void setApplicationContext(ApplicationContext applicationContext)
    throws BeansException {
		this.context = applicationContext;
		super.setApplicationContext(applicationContext);
	}

}
