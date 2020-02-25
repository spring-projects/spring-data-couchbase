package org.springframework.data.couchbase.repository;

import com.couchbase.client.core.error.IndexExistsException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.PersistenceConstructor;
import org.springframework.data.couchbase.CouchbaseClientFactory;
import org.springframework.data.couchbase.config.AbstractCouchbaseConfiguration;
import org.springframework.data.couchbase.core.mapping.Document;
import org.springframework.data.couchbase.domain.Airport;
import org.springframework.data.couchbase.domain.AirportRepository;
import org.springframework.data.couchbase.repository.config.EnableCouchbaseRepositories;
import org.springframework.data.couchbase.util.ClusterAwareIntegrationTests;
import org.springframework.stereotype.Repository;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringJUnitConfig(CouchbaseRepositoryQueryIntegrationTests.Config.class)
public class CouchbaseRepositoryQueryIntegrationTests extends ClusterAwareIntegrationTests {

  @Autowired
  CouchbaseClientFactory clientFactory;

  @Autowired
  AirportRepository airportRepository;

  @BeforeEach
  void beforeEach() {
    try {
      clientFactory.getCluster().queryIndexes().createPrimaryIndex(bucketName());
    } catch (IndexExistsException ex) {
      // ignore, all good.
    }
  }

  @Test
  void shouldSaveAndFindAll() {
    Airport vie = new Airport("airports::vie", "vie", "loww");
    airportRepository.save(vie);

    List<Airport> all = StreamSupport
      .stream(airportRepository.findAll().spliterator(), false)
      .collect(Collectors.toList());

    assertFalse(all.isEmpty());
    assertTrue(all.stream().anyMatch(a -> a.getId().equals("airports::vie")));
  }

  @Configuration
  @EnableCouchbaseRepositories("org.springframework.data.couchbase")
  static class Config extends AbstractCouchbaseConfiguration {

    @Override
    public String getConnectionString() {
      return connectionString();
    }

    @Override
    public String getUserName() {
      return config().adminUsername();
    }

    @Override
    public String getPassword() {
      return config().adminPassword();
    }

    @Override
    public String getBucketName() {
      return bucketName();
    }

  }

}
