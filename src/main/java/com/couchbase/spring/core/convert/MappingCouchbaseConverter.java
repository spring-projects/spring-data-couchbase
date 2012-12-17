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

package com.couchbase.spring.core.convert;

import com.couchbase.spring.core.CouchbaseFactory;
import com.couchbase.spring.core.mapping.CouchbasePersistentEntity;
import com.couchbase.spring.core.mapping.CouchbasePersistentProperty;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.convert.support.ConversionServiceFactory;
import org.springframework.data.mapping.context.MappingContext;

public class MappingCouchbaseConverter extends AbstractCouchbaseConverter
  implements ApplicationContextAware {

  protected ApplicationContext applicationContext;
  protected final MappingContext<? extends CouchbasePersistentEntity<?>,
      CouchbasePersistentProperty> mappingContext;

  @SuppressWarnings("deprecation")
  public MappingCouchbaseConverter(CouchbaseFactory couchbaseFactory,
    MappingContext<? extends CouchbasePersistentEntity<?>,
      CouchbasePersistentProperty> mappingContext) {
    super(ConversionServiceFactory.createDefaultConversionService());

    this.mappingContext = mappingContext;
  }

  public MappingContext<? extends CouchbasePersistentEntity<?>,
    CouchbasePersistentProperty> getMappingContext() {
    return mappingContext;
  }

  public <R> R read(Class<R> type, Object s) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  public void write(Object t, Object s) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  public void setApplicationContext(ApplicationContext applicationContext)
    throws BeansException {
    this.applicationContext = applicationContext;
  }

}
