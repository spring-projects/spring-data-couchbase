package com.example.demo;

import com.couchbase.client.java.transactions.TransactionGetResult;

public class SpringTransactionGetResult<T> {

  private final T value;
  private final TransactionGetResult inner;

  public SpringTransactionGetResult(T value, TransactionGetResult inner) {
    this.value = value;
    this.inner = inner;
  }

  public T getValue() {
    return value;
  }

  public TransactionGetResult getInner() {
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
