package com.couchbase.spring.core;


public interface DbCallback<T> {
  T doInBucket();
}
