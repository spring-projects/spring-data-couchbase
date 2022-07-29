package com.example.demo;

import com.couchbase.client.core.env.IoEnvironment;
import com.couchbase.client.core.env.SecurityConfig;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.java.codec.JacksonJsonSerializer;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.json.JsonValueModule;
import com.couchbase.client.java.transactions.config.TransactionsConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.couchbase.config.AbstractCouchbaseConfiguration;
import org.springframework.data.couchbase.repository.config.EnableCouchbaseRepositories;

import java.time.Duration;

@Configuration
@EnableCouchbaseRepositories({"com.example.demo", "com.wu.onep.ordnrt.cbviewsrch.repository"})
public class Config extends AbstractCouchbaseConfiguration {
  @Override
  public String getConnectionString() {
    return "127.0.0.1";
  }

  @Override
  public String getUserName() {
    return "Administrator";
  }

  @Override
  public String getPassword() {
    return "password";
  }

  @Override
  public String getBucketName() {
    return "travel-sample";
  }

  @Override
  public void configureEnvironment(ClusterEnvironment.Builder builder){
    builder.jsonSerializer(getSerializer())
    .securityConfig(SecurityConfig.enableNativeTls(false))
    .ioEnvironment(IoEnvironment.enableNativeIo(false));
    // builder.transactionsConfig(TransactionsConfig.durabilityLevel(DurabilityLevel.NONE));
  }

  private static JacksonJsonSerializer getSerializer() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.registerModule(new JsonValueModule());
    mapper.registerModule(new JavaTimeModule()); // this handles LocalDateTime
    mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    return JacksonJsonSerializer.create(mapper);
  }

}
