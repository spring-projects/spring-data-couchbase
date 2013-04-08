package com.couchbase.spring.core;


public interface BucketCallback<T> {
  T doInBucket();
}
