package org.springframework.data.couchbase;

import org.springframework.context.annotation.Configuration;

@Configuration
public class IntegrationTestCustomTypeKeyConfig extends IntegrationTestApplicationConfig {

  //change the name of the field that will hold type information
  @Override
  public String typeKey() {
    return "javaClass";
  }
}
