package org.springframework.data.couchbase.repository;

import org.springframework.data.annotation.Id;

/**
 * Created with IntelliJ IDEA.
 * User: michael
 * Date: 5/29/13
 * Time: 10:27 AM
 * To change this template use File | Settings | File Templates.
 */
public class User {

  @Id
  private final String key;

  private final String username;

  public User(String key, String username) {
    this.key = key;
    this.username = username;
  }

  public String getUsername() {
    return username;
  }

  public String getKey() {
    return key;
  }

}
