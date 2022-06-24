package org.springframework.data.couchbase.domain;

public class CollectionsConfig extends Config {
 @Override
  public String getScopeName(){
   return "my_scope";
 }
}
