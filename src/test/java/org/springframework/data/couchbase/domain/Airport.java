package org.springframework.data.couchbase.domain;

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.PersistenceConstructor;
import org.springframework.data.couchbase.core.mapping.Document;

@Document
public class Airport {
  @Id
  String id;

  String iata;

  String icao;

  @PersistenceConstructor
  public Airport(String id, String iata, String icao) {
    this.id = id;
    this.iata = iata;
    this.icao = icao;
  }

  public String getId() {
    return id;
  }

  public String getIata() {
    return iata;
  }

  public String getIcao() {
    return icao;
  }
}