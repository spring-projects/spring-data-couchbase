package com.example.demo;

import com.couchbase.client.core.transaction.CoreTransactionGetResult;
import com.couchbase.client.java.transactions.TransactionGetResult;

public class SpringTransactionGetResult<T> {

  private final T value;
  private final CoreTransactionGetResult inner;

  public SpringTransactionGetResult(T value, CoreTransactionGetResult inner) {
    this.value = value;
    this.inner = inner;
  }

  public T getValue() {
    return value;
  }

  public CoreTransactionGetResult getInner() {
    return inner;
  }

  @Override
  public String toString() {
    return "SpringTransactionGetResult{" +
            "value=" + value +
            ", inner=" + inner +
            '}';
  }
}
