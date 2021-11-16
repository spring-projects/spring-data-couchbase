package org.springframework.data.couchbase.transactions;

import java.util.UUID;

public class ObjectId{
  public ObjectId(){
    id = UUID.randomUUID().toString();
  }
  String id;
}
