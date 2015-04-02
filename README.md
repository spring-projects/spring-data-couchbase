# Spring Data Couchbase

The primary goal of the [Spring Data](http://www.springsource.org/spring-data) project is to make it easier to build
Spring-powered applications that use new data access technologies such as non-relational databases, map-reduce
frameworks, and cloud based data services.

The Spring Data Couchbase project aims to provide a familiar and consistent Spring-based programming model for Couchbase
Server as a document database and cache while retaining store-specific features and capabilities. Key functional areas
of Spring Data Couchbase are a POJO centric model for interacting with a Couchbase Server Bucket and easily writing a
repository style data access layer.

## Getting Help

For a comprehensive treatment of all the Spring Data Couchbase features, please refer to:

* the [User Guide](http://static.springsource.org/spring-data/couchbase/docs/current/reference/html/)
* the [JavaDocs](http://static.springsource.org/spring-data/couchbase/docs/current/api/) have extensive comments
  in them as well.
* for more detailed questions, use the [forum](http://forum.springsource.org/forumdisplay.php?f=80).

If you are new to Spring as well as to Spring Data, look for information about
[Spring projects](http://www.springsource.org/projects).


## Quick Start

### Maven configuration

Add the Maven dependency:

```xml
<dependency>
  <groupId>org.springframework.data</groupId>
  <artifactId>spring-data-couchbase</artifactId>
  <version>1.2.2.RELEASE</version>
</dependency>
```

If you'd rather like the latest snapshots of the upcoming major version, use our Maven snapshot repository and declare
the appropriate dependency version.

```xml
<dependency>
  <groupId>org.springframework.data</groupId>
  <artifactId>spring-data-couchbase</artifactId>
  <version>1.3.0.BUILD-SNAPSHOT</version>
</dependency>

<repository>
  <id>spring-libs-snapshot</id>
  <name>Spring Snapshot Repository</name>
  <url>http://repo.spring.io/libs-snapshot</url>
</repository>
```

### CouchbaseTemplate

CouchbaseTemplate is the central support class for Couchbase database operations. It provides:

* Basic POJO mapping support to and from JSON (by default through Jackson)
* Convenience methods to interact with the store (insert object, update objects) and Couchbase specific ones
* Exception translation into Spring's [technology agnostic DAO exception hierarchy](http://docs.spring.io/spring/docs/current/spring-framework-reference/html/dao.html#dao-exceptions).

### Spring Data Repositories

To simplify the creation of data repositories Spring Data COUCHBASE provides a generic repository programming model. It
will automatically create a repository proxy for you that adds implementations of finder methods you specify on an
interface.

To create a repository on top of a `User` entity, all you need to write is:

```java
public interface UserRepository extends CrudRepository<User, String> {

    /**
     * Additional custom finder method.
     */
	List<User> findByLastname(Query query);

}
```

Once you get a reference to that repository bean, you'll find a lot of methods that make it very easy o work with this
entity. In addition to the ones provided through the `CrudRepository`, you can add your own methods as well.

In general, every finder method that does not depend on a single key (like `findById`) needs a backing View on the
server side. In the example above, it assumes you have a view named `findByLastname` in the `user` design document. You
can customize the view and design document name through the `@View` annotation. Also make sure you publish them into
production before accessing it.

This is an example view for the `findByLastname` method:

```javascript
function (doc, meta) {
  if(doc._class == "com.example.entity.User" && doc.lastname) {
    emit(doc.lastname, null);
  }
}
```

You can pass in custom runtime parameters through the `Query` param.

To make the `findAll()` and `count` view work, it needs to look like this (and do not forget the `_count` reduce
function):

```javascript
function (doc, meta) {
  if(doc._class == "com.example.entity.User") {
    emit(null, null);
  }
}
```

The queries issued on execution will be derived from the method name. Extending `CrudRepository` causes CRUD methods
being pulled into the interface so that you can easily save and find single entities and collections of them.

You can have Spring automatically create a proxy for the interface by using the following JavaConfig:

```java
@Configuration
@EnableCouchbaseRepositories
public class Config extends AbstractCouchbaseConfiguration {

	@Override
	protected List<String> bootstrapHosts() {
		return Arrays.asList("host1", "host2");
	}

	@Override
	protected String getBucketName() {
		return "default";
	}

	@Override
	protected String getBucketPassword() {
		return "";
	}
}
```

This sets up a connection to a Couchbase cluster and enables the detection of Spring Data repositories (through
`@EnableCouchbaseRepositories). The same configuration would look like this in XML:

```xml
<couchbase:couchbase id="cb-first" bucket="default" password="" host="localhost" />
<couchbase:template id="cb-template-first"  client-ref="cb-first" />
<couchbase:repositories couchbase-template-ref="cb-template-first" />
```

This will find the repository interface and register a proxy object in the container. You can use it as shown below:

```java
@Service
public class MyService {

	private final UserRepository userRepository;

    @Autowired
	public MyService(UserRepository userRepository) {
		this.userRepository = userRepository;
	}

	public void doWork() {
		userRepository.deleteAll();

		User user = new User();
		user.setLastname("Jackson");

		user = userRepository.save(user);

		Query query = new Query();
		query.setKey(ComplexKey.of("Jackson"));
		List<User> allUsers = userRepository.findByLastname(query);

	}
}
```


## Contributing to Spring Data

Here are some ways for you to get involved in the community:

* Get involved with the Spring community on the Spring Community Forums.  Please help out on the
  [forum](http://forum.springsource.org/forumdisplay.php?f=80) by responding to questions and joining the debate.
* Create [JIRA](https://jira.springframework.org/browse/DATACOUCH) tickets for bugs and new features and comment and
  vote on the ones that you are interested in.
* Github is for social coding: if you want to write code, we encourage contributions through pull requests from
  [forks of this repository](http://help.github.com/forking/). If you want to contribute code this way, please reference
  a JIRA ticket as well covering the specific issue you are addressing.
* Watch for upcoming articles on Spring by [subscribing](http://www.springsource.org/node/feed) to springframework.org

Before we accept a non-trivial patch or pull request we will need you to sign the
[contributor's agreement](https://support.springsource.com/spring_committer_signup). Signing the contributor's agreement
does not grant anyone commit rights to the main repository, but it does mean that we can accept your contributions, and
you will get an author credit if we do.  Active contributors might be asked to join the core team, and given the ability
to merge pull requests.
