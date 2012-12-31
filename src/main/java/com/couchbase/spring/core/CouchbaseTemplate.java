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

import com.couchbase.client.CouchbaseClient;
import com.couchbase.spring.core.convert.CouchbaseConverter;
import com.couchbase.spring.core.convert.MappingCouchbaseConverter;
import com.couchbase.spring.core.mapping.ConvertedCouchbaseDocument;

public class CouchbaseTemplate implements CouchbaseOperations {

  private CouchbaseClient client;
  private CouchbaseConverter couchbaseConverter;

  public CouchbaseTemplate(CouchbaseClient client) {
    this(client, null);
  }

  public CouchbaseTemplate(CouchbaseClient client, CouchbaseConverter converter) {
    this.client = client;
    this.couchbaseConverter = converter == null ? getDefaultConverter(client) : converter;
  }

  private CouchbaseConverter getDefaultConverter(CouchbaseClient client) {
    MappingCouchbaseConverter converter = new MappingCouchbaseConverter(
      new CouchbaseMappingContext());
    converter.afterPropertiesSet();
    return converter;
  }

  @Override
  public void insert(Object objectToSave) {
    ConvertedCouchbaseDocument converted = new ConvertedCouchbaseDocument();
    couchbaseConverter.write(objectToSave, converted);

    client.set(converted.getId(), converted.getExpiry(), converted.getValue());
  }


}
