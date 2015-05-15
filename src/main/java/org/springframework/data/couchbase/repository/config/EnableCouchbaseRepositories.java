/*
 * Copyright 2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.couchbase.repository.config;

import org.springframework.context.annotation.ComponentScan.Filter;
import org.springframework.context.annotation.Import;
import org.springframework.data.couchbase.repository.support.CouchbaseRepositoryFactoryBean;
import org.springframework.data.repository.config.DefaultRepositoryBaseClass;

import java.lang.annotation.*;

/**
 * Annotation to activate Couchbase repositories. If no base package is configured through either {@link #value()},
 * {@link #basePackages()} or {@link #basePackageClasses()} it will trigger scanning of the package of annotated class.
 *
 * @author Michael Nitschinger
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
@Import(CouchbaseRepositoriesRegistrar.class)
public @interface EnableCouchbaseRepositories {

  /**
   * Alias for the {@link #basePackages()} attribute. Allows for more concise annotation declarations e.g.:
   * {@code @EnableCouchbaseRepositories("org.my.pkg")} instead of {@code @EnableCouchbaseRepositories(basePackages="org.my.pkg")}.
   */
  String[] value() default {};

  /**
   * Base packages to scan for annotated components. {@link #value()} is an alias for (and mutually exclusive with) this
   * attribute. Use {@link #basePackageClasses()} for a type-safe alternative to String-based package names.
   */
  String[] basePackages() default {};

  /**
   * Type-safe alternative to {@link #basePackages()} for specifying the packages to scan for annotated components. The
   * package of each class specified will be scanned. Consider creating a special no-op marker class or interface in
   * each package that serves no purpose other than being referenced by this attribute.
   */
  Class<?>[] basePackageClasses() default {};

  /**
   * Specifies which types are eligible for component scanning. Further narrows the set of candidate components from
   * everything in {@link #basePackages()} to everything in the base packages that matches the given filter or filters.
   */
  Filter[] includeFilters() default {};

  /**
   * Specifies which types are not eligible for component scanning.
   */
  Filter[] excludeFilters() default {};

  /**
   * Returns the postfix to be used when looking up custom repository implementations. Defaults to {@literal Impl}. So
   * for a repository named {@code PersonRepository} the corresponding implementation class will be looked up scanning
   * for {@code PersonRepositoryImpl}.
   *
   * @return
   */
  String repositoryImplementationPostfix() default "";

  /**
   * Configures the location of where to find the Spring Data named queries properties file. Will default to
   * {@code META-INFO/couchbase-named-queries.properties}.
   *
   * @return
   */
  String namedQueriesLocation() default "";

  /**
   * Configure the repository base class to be used to create repository proxies for this particular configuration.
   *
   * @return
   */
  Class<?> repositoryBaseClass() default DefaultRepositoryBaseClass.class;


  /**
   * Returns the {@link FactoryBean} class to be used for each repository instance. Defaults to
   * {@link CouchbaseRepositoryFactoryBean}.
   *
   * @return
   */
  Class<?> repositoryFactoryBeanClass() default CouchbaseRepositoryFactoryBean.class;

  /**
   * Configures the name of the {@link CouchbaseTemplate} bean to be used with the repositories detected.
   *
   * @return
   */
  String couchbaseTemplateRef() default "couchbaseTemplate";

}
