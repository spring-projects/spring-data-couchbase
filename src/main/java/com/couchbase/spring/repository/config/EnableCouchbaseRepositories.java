package com.couchbase.spring.repository.config;


import com.couchbase.spring.repository.support.CouchbaseRepositoryFactoryBean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.ComponentScan.Filter;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to activate Couchbase repositories. If no base package is configured through either {@link #value()},
 * {@link #basePackages()} or {@link #basePackageClasses()} it will trigger scanning of the package of annotated class.
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
