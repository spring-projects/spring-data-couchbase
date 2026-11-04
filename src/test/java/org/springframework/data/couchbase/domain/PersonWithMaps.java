package org.springframework.data.couchbase.domain;

import java.util.Map;
import java.util.Set;

import org.springframework.data.annotation.Id;
import org.springframework.data.couchbase.core.mapping.Document;
import org.springframework.data.couchbase.core.mapping.Field;

import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

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

  public void setId(String id) {
      this.id = id;
  }

  public void setVersions(Map<String, Set<String>> versions) {
      this.versions = versions;
  }

  public void setReleaseVersions(Map<String, Map<String, String>> releaseVersions) {
      this.releaseVersions = releaseVersions;
  }

  public String getId() {
      return id;
  }

  public boolean equals(Object other) {
      if (other == null || !(other instanceof PersonWithMaps)) {
          return false;
      }
      PersonWithMaps that = (PersonWithMaps) other;
      return equals(this.getId(), that.getId()) && equals(this.versions, that.versions)
              && equals(this.releaseVersions, that.releaseVersions);
  }

  boolean equals(Object s0, Object s1) {
      if (s0 == null && s1 == null || s0 == s1) {
          return true;
      }
      Object sa = s0 != null ? s0 : s1;
      Object sb = s0 != null ? s1 : s0;
      return sa.equals(sb);
  }

}
