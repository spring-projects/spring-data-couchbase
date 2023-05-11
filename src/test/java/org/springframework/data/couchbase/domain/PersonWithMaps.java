package org.springframework.data.couchbase.domain;

import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.couchbase.core.mapping.Document;
import org.springframework.data.couchbase.core.mapping.Field;

import java.util.Map;
import java.util.Set;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@Document
public class PersonWithMaps {

  @Id
  private String id;
  @Field
  private Map<String, Set<String>> versions;
  @Field
  private Map<String, Map<String,String>> releaseVersions;

  public PersonWithMaps(){

  }
}
