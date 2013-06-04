# Spring Data Couchbase

This project adds common Spring Data functionality (like configuration, templates and repositories) on top of the high-performance, scalable and flexible architecture of Couchbase. It makes it especially easy to work with POJO entities, query views and work with them in a natural manner. Developers coming from a relational database will find it easier to get started with Couchbase, while still gaining lots of performance and scalability improvements. JSON is used as the underlying storage inside Couchbase Server, so Views can be used to further enhance query mechanisms.

Full documentation is still in the making, so this README outlines the basic steps you need to do in order to get it up and running. It also provides a very rough overview of its capabilities. Note that this library is still in the making and does not provide all features yet.

## Features
### Implemented

 - Templates
 - JavaConfig
 - CRUD Repository (aside *All and count methods, see planned)
 - Basic Auditing (JMX) 
 - Additional: transparent @Cacheable support

### Planned (before 1.0)

 - Mapping of arbitrary Objects (Value Objects)
 - View support in template
 - XML Config (namespace for template + repositories)
 - find*-based methods on repositories through Views
 - @View annotation for customization
 
### Planned (after 1.0)
 
 - Relationship Support
 - Dynamic View Generation

## Installation
The preferred way is to install the package through maven:

```xml
<dependency>
  <groupId>org.springframework.data</groupId>
  <artifactId>spring-data-couchbase</artifactId>
  <version>1.0.0.M1</version>
</dependency>
```
Note that the first milestone is not yet released, but will be available through maven central once its done. This will pull in all dependencies needed, including the Couchbase Java SDK and the Spring dependencies.


## Configuration
Since we are in the Spring ecosystem, you can either configure it through XML or plain Java (often referred to as JavaConfig).

### XML-based configuration
This will be added once the XML namespace is implemented. You can use plain beans in the meantime like this:

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

### JavaConfig

If you like to configure your Spring environment through POJOs, JavaConfig is the way to go. While you could define your beans for templates and such directly, we provide a `AbstractCouchbaseConfig` to make it even easier for you. 

Note that if you want to make use of JavaConfig, you need to put the `cglib` into your classpath:

```xml
<dependency>
   <groupId>cglib</groupId>
   <artifactId>cglib</artifactId>
   <version>2.2</version>
</dependency>
```

You only need to provide a bean that refers to the `CouchbaseClient` instance like this:

```java
@Configuration
public class ApplicationConfig extends AbstractCouchbaseConfiguration {

  @Bean
  public CouchbaseClient couchbaseClient() throws Exception {
    return new CouchbaseClient(
      Arrays.asList(new URI("http://localhost:8091/pools")),
      "default",
      ""
    );
  }

}
```

Likely, you want to have Repository support (and not only use the template directly). To enable repositories, add the `@EnableCouchbaseRepositories` annotation and give it the namespace where it should search for your repositories:

```java
@Configuration
@EnableCouchbaseRepositories("com.example.business.repositories")
public class ApplicationConfig extends AbstractCouchbaseConfiguration {
...
}
```
This will make the repositories automatically available in your `@Autowired` annotations or directly through the context.

If you want to make use of the Caching support provided through the @Cacheable annotations, then you want to configure the CacheManager and add the `@EnableCaching` annotation:

```java
@Configuration
@EnableCaching
public class ApplicationConfig extends AbstractCouchbaseConfiguration {

  @Bean
  public CouchbaseClient couchbaseClient() throws Exception {
    return new CouchbaseClient(
      Arrays.asList(new URI("http://localhost:8091/pools")),
      "default",
      ""
    );
  }

  @Bean
  public CouchbaseCacheManager cacheManager() throws Exception {
    HashMap<String, CouchbaseClient> instances = new HashMap<String, CouchbaseClient>();
    instances.put("persistent", couchbaseClient());
    return new CouchbaseCacheManager(instances);
  }

}
```

## Usage
Note that the usage guide in this README is intentionally kept very sparse. You'll find much more information in the documentation once its provided.

### Repositories
The topmost layer of indirection is the Repository abstraction. It provides convenient CRUD access on top of your Entities. If you need more direct access to Couchbase, look at the next chapter.

For a generic introduction into Spring Data Repositories, look [here](http://static.springsource.org/spring-data/data-jpa/docs/current/reference/html/repositories.html).

Create a Entity and a Repository that uses it as its generic type. This will setup everything for you (don't forget to add the proper `@EnableCouchbaseRepositories` with the correct namespace).

A sample Entity (getter and setter omitted):

```java
package com.example.business.entities;

import org.springframework.data.annotation.Id;
import org.springframework.data.couchbase.core.mapping.Field;


public class BlogPost {

  @Id
  private String id;

  private String title;
  private String content;
  private String author;

  @Field("pub")
  private boolean published;

  public BlogPost(String title, String content, String author, boolean published) {
    id = "post:" + title.toLowerCase().replace(" ", "-");
    this.title = title;
    this.content = content;
    this.author = author;
    this.published = published;
  }

}

```

Now, we can create a Repository on top of it:

```java
package com.example.business.repositories;

import org.springframework.data.couchbase.repository.CouchbaseRepository;
import com.example.business.entities.BlogPost;

public interface BlogPostRepository extends CouchbaseRepository<BlogPost, String> {
}
```

Jup - thats it! Just by doing this, you'll get full CRUD support on top of your entity. You can now either `@Autowire` the repository in a service class or access it directly from the context.

```java
public class BlogPostService {
  @Autowired
  public BlogPostRepository repository;
}
```

```java
ApplicationContext context = new AnnotationConfigApplicationContext(ApplicationConfig.class);
BlogPostRepository repository = context.getBean(BlogPostRepository.class);

// Find a BlogPost by its id
BlogPost post = bean.findOne("blogpost:my-id");
```

Note that once we finish find* support through views, more information will be provided here.

### Template
You can also use the template directly, which gets you one step "closer to the metal". It will still do some Object mapping for you, but you need to be more specific on what you want to get done.

You can again autowire it or access it directly from your beans:

```java
CouchbaseOperations ops = context.getBean("couchbaseTemplate", CouchbaseOperations.class);
```

The template provides lots of CRUD methods, as well as more direct access to Views. Exceptions are translated and connection management is handled for you in a more straightforward way.

```java
BlogPost post1 = new BlogPost("My Title", "Long Content", "Michael", true);
ops.insert(post1);
```

### Caching
While not directly related to Spring Data, the `@Cacheable` annotations in spring provide a very easy and transparent caching mechanism. Note that since you can cache everything, those objects get stored as serialized Java objects into Couchbase and are therefore not directly accessible by Views or the Template class. They provide a very nice mechanism to cache any kind of expensive operations (relational db calls, view rendering results, html,…).

First, configure it as seen in the Configuration section. Then, you can put the `@Cacheable` annotation around any method and it will transparently cache the results for you:

```java
public class ComplexComputations {

  @Cacheable(value="persistent", key="'longrunsim-'+#time")
  public String simulateLongRun(long time) {
    try {
      Thread.sleep(time);
    } catch(Exception ex) {
      System.out.println("This shouldnt happen...");
    }
    return "{\"Sleeping\": \""+time+"\"}";
  }

}
```

Then access it:

```java
String result = complex.simulateLongRun(2000);
```

If you look at the Couchbase UI, you will see get and set requests going on that transparently store and retreive the information. You can find all details about this caching mechanism [here](http://static.springsource.org/spring/docs/3.2.2.RELEASE/spring-framework-reference/html/cache.html).

## Contributing & Troubleshooting
Here are some ways for you to get involved in the community:

* Get involved with the Spring community on the Spring Community Forums.  Please help out on the [forum](http://forum.springsource.org/forumdisplay.php?f=80) by responding to questions and joining the debate.
* Create [JIRA](https://jira.springframework.org/browse/DATACOUCH) tickets for bugs and new features and comment and vote on the ones that you are interested in.  
* Github is for social coding: if you want to write code, we encourage contributions through pull requests from [forks of this repository](http://help.github.com/forking/). If you want to contribute code this way, please reference a JIRA ticket as well covering the specific issue you are addressing.
* Watch for upcoming articles on Spring by [subscribing](http://www.springsource.org/node/feed) to springframework.org
* Write blog posts and articles about it! Send them over to us so we can link them properly.

Before we accept a non-trivial patch or pull request we will need you to sign the [contributor's agreement](https://support.springsource.com/spring_committer_signup).  Signing the contributor's agreement does not grant anyone commit rights to the main repository, but it does mean that we can accept your contributions, and you will get an author credit if we do.  Active contributors might be asked to join the core team, and given the ability to merge pull requests.


