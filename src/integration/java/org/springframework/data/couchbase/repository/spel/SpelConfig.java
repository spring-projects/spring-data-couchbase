package org.springframework.data.couchbase.repository.spel;

import java.util.Collections;
import java.util.Map;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.couchbase.IntegrationTestApplicationConfig;
import org.springframework.data.couchbase.repository.config.EnableCouchbaseRepositories;
import org.springframework.data.repository.query.spi.EvaluationContextExtension;
import org.springframework.data.repository.query.spi.EvaluationContextExtensionSupport;

@Configuration
@EnableCouchbaseRepositories
public class SpelConfig  extends IntegrationTestApplicationConfig {

  @Bean
  public EvaluationContextExtension customSpelExtension() {
    return new CustomSpelExtension();
  }

  public static class CustomSpelExtension extends EvaluationContextExtensionSupport {

    /**
     * Returns the identifier of the extension. The id can be leveraged by users to fully qualify property lookups and
     * thus overcome ambiguities in case multiple extensions expose properties with the same name.
     *
     * @return the extension id, must not be {@literal null}.
     */
    @Override
    public String getExtensionId() {
      return "custom";
    }

    @Override
    public Map<String, Object> getProperties() {
      return Collections.<String, Object>singletonMap("oneCustomer", "uname-3");
    }
  }

}
