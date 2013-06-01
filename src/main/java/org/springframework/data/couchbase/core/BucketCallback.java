package org.springframework.data.couchbase.core;


public interface BucketCallback<T> {
  T doInBucket();
}
