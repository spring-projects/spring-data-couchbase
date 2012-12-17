Couchbase Java Spring Integration
=================================

This project aims to bridge the gap between Spring and the Couchbase Java SDK. Currently, only Caching is supported but more features (like support for Spring Data) will be added in the near future. This project has been extracted from the Client SDK to provide a more flexible release cycle and to prevent the core SDK to be messed up with too much dependencies.

Installation
------------
Currently, you need to checkout the source code from here and build it on your on. In the future, this project will be distributed as a maven package and can be included as follows:

```xml
<dependencies>
<dependency>
  <groupId>couchbase</groupId>
  <artifactId>spring-data-couchbase</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
```

It is distributed from the [Couchbase Maven Repository](http://files.couchbase.com/maven2/):

```xml
<repositories>
  <repository>
    <id>couchbase</id>
    <name>Couchbase Maven Repository</name>
    <layout>default</layout>
    <url>http://files.couchbase.com/maven2/</url>
    <snapshots>
      <enabled>false</enabled>
    </snapshots>
  </repository>
</repositories>
```
Currently, the project depends on the following packages:

 * couchbase.couchbase-client: 1.1.0
 * org.springframework.spring-context: 3.1.3.RELEASE
 * cglib.cglib: 2.2.2
 * (When Testing) junit.junit: 4.11

You don't need to download them by hand since they are resolved through Maven automatically.

Dependency Injection Basics (Spring IoC)
----------------------------------------
Technically, you can use the Spring Beans without the `couchbase-spring` project, but it is crucial to understand how it works because you'll need it for more advanced topics like caching.

Normally, you would instantiate the `CouchbaseClient` by its constructor in your Java code. When working with beans, you need to define a bean that handles the construction for you like this:

```xml
<bean id="couchbaseClient" class="com.couchbase.client.CouchbaseClient">
  <constructor-arg name="baseList">
      <list>
          <bean id="firstURI" class="java.net.URI">
              <constructor-arg value="http://127.0.0.1:8091/pools" />
          </bean>
      </list>
  </constructor-arg>
  <constructor-arg name="bucketName" value="default" />
  <constructor-arg name="pwd" value="" />
</bean>
```

This is equivalent to the following Java code:

```java
ArrayList<URI> baseList = new ArrayList<URI>();
baseList.add(URI.create("http://127.0.0.1:8091/pools"));
String bucketName = "default";
String pwd = "";
CouchbaseClient client = new CouchbaseClient(baseList, bucketName, pwd);
```

If you want to access the bean from your Java code, you can do it like this (you need to make sure that the `beans.xml` file is reachable from your `CLASSPATH`):

```java
ApplicationContext context = new ClassPathXmlApplicationContext("beans.xml");
CouchbaseClient client = context.getBean("CouchbaseClient", CouchbaseClient.class);
```

Here, the `beans.xml` defines the bean we saw previously. Spring handles the construction of the object for us and we can then proceed to call the well-known methods on the object. You'll need this kind of approach when you want to use Couchbase for more parts then - lets say - Caching in your project. If you just want to use Caching, you don't even need to work with the CouchbaseClient directly at all.

Also, if you prefer Java-Style configs you can define a class like this (this is the equivalent to the xml shown above):

```java
@Configuration
class ApplicationConfig {

 @Bean
 public CouchbaseClient couchbaseClient() {
   return new CouchbaseClient(Arrays.asList(URI.create("http://127.0.0.1:8091/pools")), "default", "");
 }

 @Bean
 public CouchbaseCacheManager cacheManager() {

   HashMap<String, CouchbaseClient> instances = new HashMap<String, CouchbaseClient>();
   instances.put("test", couchbaseClient());

   return new CouchbaseCacheManager(instances);
 }
}
```

This also includes the config for the `CouchbaseCacheManager` that will be used in the next chapter. You can then go ahead and include it like this:

```java
ApplicationContext context = new AnnotationConfigApplicationContext(ApplicationConfig.class);
CouchbaseClient client = context.getBean("couchbaseClient", CouchbaseClient.class);
```

Caching with Couchbase in Spring
--------------------------------
[Caching in Spring](http://static.springsource.org/spring/docs/3.1.x/spring-framework-reference/html/cache.html) mainly works bei annotating your cachable entities with the `@Cacheable` annotation. If you give it only a name like `@Cacheable("default")`, then it tries to use the `default` cache configuration. Before we can use it though, we need to define it.

Look at the folling bean configuration, which we'll break down afterwards:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<beans xmlns="http://www.springframework.org/schema/beans"
        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
        xmlns:cache="http://www.springframework.org/schema/cache"
        xmlns:context="http://www.springframework.org/schema/context"
        xmlns:util="http://www.springframework.org/schema/util"
        xmlns:p="http://www.springframework.org/schema/p"
        xsi:schemaLocation="http://www.springframework.org/schema/beans
                            http://www.springframework.org/schema/beans/spring-beans.xsd
                            http://www.springframework.org/schema/cache
                            http://www.springframework.org/schema/cache/spring-cache.xsd
                            http://www.springframework.org/schema/context
                            http://www.springframework.org/schema/context/spring-context-3.0.xsd
                            http://www.springframework.org/schema/util
                            http://www.springframework.org/schema/util/spring-util.xsd">

    <cache:annotation-driven />
    <context:annotation-config/>

    <bean id="couchbaseClient" class="com.couchbase.client.CouchbaseClient">
      <constructor-arg name="baseList">
          <list>
              <bean id="firstURI" class="java.net.URI">
                  <constructor-arg value="http://127.0.0.1:8091/pools" />
              </bean>
          </list>
      </constructor-arg>
      <constructor-arg name="bucketName" value="default" />
      <constructor-arg name="pwd" value="" />
    </bean>

   <bean id="cacheManager" class="com.couchbase.spring.cache.CouchbaseCacheManager">
    <constructor-arg>
        <util:map>
            <entry key="default" value-ref="couchbaseClient" />
        </util:map>
    </constructor-arg>
   </bean>

</beans>
```

You should be able to identify the `couchbaseClient` bean, which we'll defined prevously. Here the important part is the `cacheManager` bean that spring will pick up automatically when the `<cache:annotation-driven />` directive is found. The only configuration that you have to do is to tell the 
`CouchbaseCacheManager` (who orchestrates the caches for you), which `CouchbaseClient` instances you want to map to which `name` (the `key`). In the example above, every time you use the `@Cacheable("default")` annotation, the `couchbaseClient` connection defined above is used to store and read the cache values. Since this is essentially a `HashMap`, you can add as many instances as you want, but keep in mind that you should mainly stick to one instance (bucket). Therefore, the configuration shown above should suffice most use cases (of course, please adapt the `CouchbaseClient` constructor params according to your environment).

That's all it takes to have Couchbase cache your objects. Now we can go through a quick example to show how it works. Assume the following class inside the `com.couchbase.example` package:

```java
package com.couchbase.spring;

import org.springframework.cache.annotation.Cacheable;

public class Bookstore {

   @Cacheable("default")
   public String helloWorld(String name) {
     return "Hello " + name + "!";
   }
}
```

To make Spring pick up this class, add the following bean to your config:

```xml
<bean class="com.couchbase.example.Bookstore" id="bookstore" />
```

Consider the following simple application that makes use of the `Bookstore`:

```java
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class App {

    @Autowired
    static Bookstore bookstore;

    public static void main( String[] args ) {
      ApplicationContext context = new ClassPathXmlApplicationContext("beans.xml");
      bookstore = context.getBean("bookstore", Bookstore.class);

      System.out.println(bookstore.helloWorld("World"));
      System.out.println(bookstore.helloWorld("World"));
      System.out.println(bookstore.helloWorld("Michael"));

    }
}
```

We load up our `beans.xml` and get the referenced `bookstore` object out of it. Since the object is now under control of spring, it will pick up our annotation and when we call the `helloWorld()` method for the first time, the resulting object is serialized and stored in Couchbase. On the second try with the same argument, the method itself is not called anymore but loaded directly out of Couchbase! On the third call, since the argument is different, the new value is stored again in Couchbase (you can check the stored values inside the bucket through the Couchbase Server Admin UI).

Be aware of the following things:

 * Objects are stored as serialized Java objects, not as JSON. You won't be able to read the values through the UI.
 * If you use more than one argument, Spring will create a random key for it (see the [docs](http://static.springsource.org/spring/docs/3.1.x/spring-framework-reference/html/cache.html)).
 * There are lots of other options available, again read the documentation for it.

Currently, the customization context is very limited, but there is more functionality planned in the future. We'd love to hear your ideas and needs!