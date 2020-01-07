package org.springframework.data.couchbase.repository.extending.method;

import org.springframework.data.annotation.Id;

class MyItem {
  @Id
  public final String id;

  public final String value;

  public MyItem(String id, String value) {
    this.id = id;
    this.value = value;
  }
}
