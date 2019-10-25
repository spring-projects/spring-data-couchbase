package org.springframework.data.couchbase.repository;


import org.springframework.data.annotation.Id;

public class Item {

  @Id
  public String id;

  public String description;

  public Item(String id, String description) {
    this.id = id;
    this.description = description;
  }

  public Item() {}

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    Item item = (Item) o;

    if (!id.equals(item.id)) return false;
    return !(description != null ? !description.equals(item.description) : item.description != null);

  }

  @Override
  public int hashCode() {
    int result = id.hashCode();
    result = 31 * result + (description != null ? description.hashCode() : 0);
    return result;
  }
}
