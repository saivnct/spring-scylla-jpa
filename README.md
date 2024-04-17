# [Giangbb Studio] - spring-scylla-jpa driver

spring-scylla-jpa driver driver makes working with Scylla in go easy and less error-prone.
It’s developed based on [Scylla Java Driver for Scylla and Apache Cassandra](https://github.com/scylladb/java-driver) with supporting ORM feature

## Features
* This Scylla Java Driver is Shard Aware and contains extensions for a tokenAwareHostPolicy. Using this policy, the driver can select a connection to a particular shard based on the shard’s token. As a result, latency is significantly reduced because there is no need to pass data between the shards.
* Auto create table if not exists
* Auto create index if not exists
* Auto create UDT if not exists
* Build repositories based on common Repository Data interfaces
* Support for synchronous, and asynchronous data operations
* Exception Translation to the familiar Spring DataAccessException hierarchy
* Building statements with QueryBuilder's Fluent API. Use of Fluent API allows easier build of complex queries, as opposed to use of hardcoded query strings.

## Getting started

Here is a quick teaser of an application using Spring Data Repositories in Java:
```java
@Configuration
public class ScyllaConfiguration extends AbstractScyllaConfiguration {
    
    @Override
    public String getKeyspaceName() {return "springdemo";}

    @Override
    @NonNull
    public String getLocalDataCenter() {return "datacenter1";}

    @Override
    protected String getContactPoints() {return "localhost";}

    @Override
    public SchemaAction getSchemaAction() {return SchemaAction.valueOf("CREATE_IF_NOT_EXISTS");}
    
    @Override
    public String[] getEntityBasePackages() {
        return new String[]{"studio.giangbb.scylladbdemo.entity"};
    }
}

@CqlName("user")
@Table
@Entity
@NamingStrategy(convention = SNAKE_CASE_INSENSITIVE)
public class User {
    @PartitionKey
    private UUID id;

    @ClusteringColumn
    private int userAge;

    @Indexed
    private String userName;
    
    @Indexed
    private UserTupleIndex userTupleIndex;

    private UserTuple userTuple;
    
    @Computed("writetime(user_name)")
    private long writetime;

    public User() {
    }

    public User(String userName, int userAge, UserTupleIndex userTupleIndex, UserTuple userTuple) {
        this.id = Uuids.timeBased();
        this.userName = userName;
        this.userAge = userAge;
        this.userTupleIndex = userTupleIndex;
        this.userTuple = userTuple;
    }
}

@Component
@Qualifier("userDAO")
public class UserDAO extends SimpleScyllaRepository<User> {

    @Autowired
    public UserDAO(ScyllaTemplate scyllaTemplate) {
        super(User.class, scyllaTemplate);
    }
}
```

Add the Maven dependency:
```xml
<dependency>
    <groupId>com.giangbb</groupId>
    <artifactId>scylla-jpa</artifactId>
    <version>${version}</version>
</dependency>
```

## Building from Source
```bash
    mvn clean install
```