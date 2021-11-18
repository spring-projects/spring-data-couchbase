package org.springframework.data.couchbase.transactions;

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Version;

class AirlineNoKey {
  @Id
  public String id;
  public String name;
  @Version // repository.save() needs version to distinguish insert/replace/upsert for tx/no-tx
  public long version;

  public AirlineNoKey(String id, String name) {
    this.id = id;
    this.name = name;
  }

  AirlineNoKey copy() {
    AirlineNoKey b = new AirlineNoKey(id,
        name);
    b.version = version;
    return b;
  }

  AirlineNoKey withName(String newName) { // with-er to change the name
    AirlineNoKey b = copy();
    b.name = newName;
    return b;
  }

  public String toString() {
    return "{ id: " + id + ", name: \"" + name + "\" version: " + version + " }";
  }

}
