# invesdwin-context-persistence
This project provides persistence modules for the [invesdwin-context](https://github.com/subes/invesdwin-context) module system.

## Maven

Releases and snapshots are deployed to this maven repository:
```
http://invesdwin.de/artifactory/invesdwin-oss
```

Dependency declaration:
```xml
<dependency>
	<groupId>de.invesdwin</groupId>
	<artifactId>invesdwin-context-persistence-jpa-hibernate</artifactId>
	<version>1.0.0-SNAPSHOT</version>
</dependency>
```

## JPA Modules

The `invesdwin-context-persistence-jpa` module provides a way to code against JPA (Java Persistence API) without having to bind yourself to a specific ORM (Object-Relational-Mapping) framework. In theory you could use the same entities you programmed and switch between Hibernate, OpenJPA, EclipseLink, Datanucleus, Kundera and so on. In practice you will find out that not all JPA implementations are equally good. So for now we only have modules available for [Hibernate](http://hibernate.org/) (see `invesdwin-context-persistence-jpa-hibernate`) as the most mature JPA implementation for relational databases in our opinion. Additionally there currently are experimental modules for [Datanucleus](http://www.datanucleus.org/) (see `invesdwin-context-persistence-jpa-datanucleus`) and [Kundera](https://github.com/impetus-opensource/Kundera) (see `invesdwin-context-persistence-jpa-kundera`) as they provide connectors to NoSQL databases over JPA. These are in experimental stage since they still fail some of our unit tests for JPA compliance or have some other bugs. But they can already be used when you forego advanced JPA features. For now they will have to suffice as a proof of concept that `invesdwin-context-persistence-jpa` stays neutral to any JPA implementation specifically and provides the flexibility to give up on extended JPA features. So the rest of this documentation will tell you what you can do with a full JPA implementation like Hibernate and will give you hints on how you can scale down for different requirements. The main JPA module provides the following tools:

### Entities

- **AEntity**: this is the base class class for entities that use optimistic locking, timestamps for creation and last update, as well as a simple numerical ID based on a sequence per table (currently only available in the Hibernate module), as this is the best performing strategy for high load systems. There are a few variations available that you can use as alternative base classes for fine tuning:
  - `AEntityWithIdentity`: using the identity column feature of your database if supported as the ID generator strategy
  - `AEntityWithTableSequence`: using a table for sequences which should be supported by any database
  - `AEntityWithSequence`: using the high performance variation of one sequence per table in Hibernate, or the default sequence strategy on other ORMs (this is the default base class for `AEntity`)
- **AUnversionedEntity**: this is the base class that only implements the ID column itself, without adding optimistic locking or timestamps. It is useful when you have a very large table where you want to spare you the hard disk space for uneccessary additional fields on millions of rows. As above you have the option to use other ID generator strategies by extending `AUnversionedEntityWithIdentity` or `AUnversionedEntityWithTableSequence`, while `AUnversionedEntityWithSequence` is the default.

### Data Access Objects (DAOs)

- **ADao**: this is the default DAO implementation. It is always bound to provide queries for one specific entity.  It is the default implementation for a numerical ID as used in AEntity and its other variations. The DAO provides support for [spring-data-jpa](http://projects.spring.io/spring-data-jpa/) and [QueryDSL](http://www.querydsl.com/) while extending it with convenience methods for [Query by Example](https://en.wikipedia.org/wiki/Query_by_Example). To get the most out of your DAOs, your entities should follow the following suggestions:
  - only use basic Java object types as fields in your entities and convert to/from a different desired type in the getters and setters. E.g. use a Date/BigDecimal fields and convert to/from FDate/Decimal (see [invesdwin-util](https://github.com/subes/invesdwin-util#types)) in the getter/setter methods. So you don't have to setup ORM specific type converters (since this is missing in the JPA specification).
  - also make sure you do not use primitive data types such as long, double but instead object types like Long, Double for fields, so that the null checks can work properly in validation phase and for Query by Example convenience.
  - prefer to use `@Enumerated(EnumType.STRING)` for enum fields so that you can reorder the enum values in your code without needing to migrate the ordinal values in your existing database rows
  - utilize BeanValidation annotations extensively to only allow clean data into your database
  - put the invesdwin `@Indexes` annotation on your entities to let the framework generate them for you (since the JPA specification is missing this and every ORM has its own mechanism for this, we provide yet another mechanism)
  - if you want to roll your own customized entities, just do that and implement the `IEntity` interface to still be able to use our ADao base classe, which needs to have a way to retrieve the numerical IDs.
- **ACustomIdDao**: this DAO implementation can be used when you want to use something different as an ID in combination with your custom entity that not necessarily has a numeric ID (otherwise you could use `ADao` with `IEntity`). E.g. when you want to use a composite primary key as the ID.
- **ARepository**: for a good design, DAOs should only contain queries for their specific entity. If you want to write queries that span multiple entities, you should rather externalize them into a repository implementation, which can also reference multiple DAOs to get its work done. Or you can write a repository for a special task like complex searching over different entities that requires a whole bunch of code to write the queries. Regarding queries here are a few tips:
 - favor QueryDSL over JPQL for complex queries, since that has the advantage of being type safe and thus robust against refactorings in your entities (the `Q...Entity` classes get generated via an annotation processor and will give compile errors for queries which you have to adjust)
  - use Query by Example for 80% of your simple query needs
  - leverage the query cache and JPA Level 2 cache where appropriate instead of setting up your own cache (per default backed by EhCache in the Hibernate module)
  - use the `BulkInsertEntities` class to push bulk insert speeds to the maximum. It leverages the LOAD DATA INFILE on a MySQL database or at least does proper batch inserts for other databases
  - put the spring `@Transactional` annotation around methods that do DML operations on the database, since per default only read only transactional attributes are applied. You should also put `@Transactional` annotations around methods in your services/beans that do database operations over multiple repositories and DAOs, so that all operations share the same transaction and get rolled back together. Multiple persistence units are also supported, since the `ContextDelegatingTransactionManager` takes care of shared transactions for you. Since `invesdwin-context` is using AspectJ you can even put `@Transactional` inside your `@Configurable` non-bean objects that you instantiate yourself.
- **JpaRepository**: since spring-data-jpa is used, you can also define query interfaces that extend `JpaRepository` and leverage the magic spring provides. The `invesdwin-context` application bootstrap will generate the spring-xml configuration for all interfaces in the classpath that extend `JpaRepository` (which do not extend `IDao`) automatically for you and make them reference the correct persistence unit for transactions depending on the entity class given in the `JpaRepository` generic type arguments. It will look up the `@PersistenceUnit` in the entity class itself to determine the bean names for the datasource, transaction manager and so on. See the next paragraphs for the more details on this automated configuration mechanism.

### Testing and Configuration

- **persistence.log**: per default our transaction manager logs SQL statements into a log file. It adds information about transactions, so you can always troubleshoot your persistence issues properly. If you want to gain some additional performance during production use, just change the log level of `org.jdbcdslog.StatementLogger` to `OFF` in your logback configuration.
- **@PersistenceTest**: per default all JUnit tests run in an in-memory H2 database. Add this annotation to your test case to switch to a real server during testing (e.g. when you want to test against real data you seeded during development of a website). The default non in-memory server is supposed to be a local MySQL instance that is setup with the following sql script:
```sql
USE mysql;
CREATE USER 'invesdwin'@'localhost' IDENTIFIED BY 'invesdwin';
GRANT ALL ON invesdwin.* TO 'invesdwin'@'localhost';
GRANT SUPER ON *.* TO 'invesdwin'@'localhost';
CREATE DATABASE invesdwin;
```
- **Install DB**: to install a MySQL instance on ubuntu, use the following commands:
```bash
sudo apt-get install mysql-server mysql-workbench
# increase some limits
sudo sed -i s/max_connections.*/max_connections=1000/ /etc/mysql/my.cnf
echo "innodb_file_format=Barracuda" | sudo tee -a /etc/mysql/my.cnf
sudo /etc/init.d/mysql restart
```
- **Reset DB**: to reset the test database schema (deleting all tables), run the following command on the pom.xml of `invesdwin-context-persistence-jpa`:
```bash
mvn -Preset-database-schema process-resources
```
- **Change DB Config**: to use a different database as a test server, just put a file called `/META-INF/env/${USERNAME}.properties` into your classpath. This file allows you to override the modules default configuration depending on your local development environment (for more information and on how to define distribution specific settings, see the [invesdwin-context documentation](https://github.com/subes/invesdwin-context#tools). Just put following properties with changed values there (and make sure the connection driver is added as a maven dependency and available in the classpath, you can also deploy an embedded database like this):
```properties
de.invesdwin.context.persistence.jpa.PersistenceUnitContext.CONNECTION_DRIVER@default_pu=com.mysql.jdbc.Driver
de.invesdwin.context.persistence.jpa.PersistenceUnitContext.CONNECTION_URL@default_pu=jdbc:mysql://localhost:3306/invesdwin
de.invesdwin.context.persistence.jpa.PersistenceUnitContext.CONNECTION_USER@default_pu=invesdwin
de.invesdwin.context.persistence.jpa.PersistenceUnitContext.CONNECTION_PASSWORD@default_pu=invesdwin
de.invesdwin.context.persistence.jpa.PersistenceUnitContext.CONNECTION_DIALECT@default_pu=MYSQL
```
- **Multiple Persistence Units**: notice the `@default_pu` string in the properties? This defines the persistence unit this configuration is for. Duplicating these values and changing the name of the persistence unit allows you to setup multiple databases to be used in one application. Depending on the dialect and which persistence provider modules you have in your classpath (e.g. Hibernate, Datanucleus, Kundera) you can in theory even mix Hibernate for relational databases, Datanucleus for HBase and Kundera for Cassandra as multiple ORMs in the same application. Though we still have to define a few more `invesdwin-persistence-jpa-*` modules and create an actual proof of concept for this feature, but it is an interesting idea regarding polyglot persistence. For now this just allows you to e.g. use multiple databases with the Hibernate module.
  - you can annotate your entity class with `@PersistenceUnit("another_pu")` to tell the invesdwin-context bootstrap to associate this entity with the specific persistence unit configuration
  - the `ACustomIdDao`/`ADao` class will lookup its entity class to determine the persistence unit (see `PersistenceProperties` class) to be used for its entity manager and transactions. Since `ARepository` does not have an associated entity, you have to provide the proper persistence unit name by overriding the `getPersistenceUnitName()` method. 
  - the `IPersistenceUnitAware` interface defines this method and `ARepository` implements this interface. You can actually also implement this interface inside any of your spring Beans/Services to add this information. It tells the `ContextDelegatingTransactionManager` to use this specific persistence unit for the transactions defined inside this class
  - you can have shared transactions spanned over multiple persistence units simply by nesting those transactions in one another. If the nested transaction gets rolled back, the outer transaction will be rolled back as well. Still you have to be careful about nested transactions being committed before the outer transaction gets committed, so choose the nesting levels wisely and test your rollback scenarios sufficiently. JTA with a two-phase-commit would have been an interesting alternative here, but setting this up with support for NoSQL databases and multiple ORMs is a nightmare sadly.
  - additionally you can override/specify the persistence unit to use directly with a `@Transactional(value="another_tm")` annotation. This defines a specific transaction manager bean to be used for this transaction definition. It leverages the naming convention of all persistence unit related beans following a specific naming pattern:
    - `another_pu` defines the persistence unit name `another` (`default_pu` being the `default` persistence unit) and is required to always carry the suffix `_pu` so it can be identified as such
    - `another_tm` defines the transaction manager bean and can be used in `@Transactional` annotations as above or to lookup the specific `PlatformTransactionManager` bean to do manual transaction handling by calling its `getTransaction`/`commit`/`rollback` methods
    - `another_emf` defines the `EntityManagerFactory` bean with which you can retrieve `EntityManager` instances. You can make them threadsafe with springs `SharedEntityManagerCreator` or directly use `PersistenceProperties.getPersistenceUnitContext("another_pu").getEntityManager()` to get a thread safe instance.
    - `another_ds` defines the `DataSource` which normally is a [c3p0](http://www.mchange.com/projects/c3p0/) connection pool instance that can be used to gain direct JDBC access to the database. for now c3p0 is the most robust connection pool we found, even if it is not said to be the fastest. At least it provides a `PreparedStatement` cache (which is missing in HikariCP and hard to configure sometimes depending on the JDBC driver), does not give unexplainable runtime exceptions in production environments (we had some encounters of the weird kind with BoneCP) and provides helpful errors when it encounters transaction deadlocks (which can be very helpful during development and troubleshooting of production problems). In most cases we value development comfort higher than raw speed (which definitely comes at a cost), so that is why we chose c3p0 in the end. If you want to use a different connection pool we could provide some means to allow you to override the connection pool factory, but for now it cannot be changed since we did not have the use case for this feature yet (we normally only add variability into the modules as soon/late as we need it).
- **Data Management in Tests**: you can extend `APersistenceTestPreparer` to build some fixture generators that you can use inside your setup of unit tests to get some test data. By defining a separate class for the preparer, you can even run it as a unit test itself to populate a test database (configured by annotating the test class with `@PersistenceTest(PersistenceTestContext.SERVER)`). You can also run `new PersistenceTestHelper().clearAllTables()` to reset the database inside your unit test as often as you want (e.g. in your setUp() or setUpOnce() method). It goes over all spring beans that implement `IClearAllTablesAware` and deletes all tables rows with this since the ADao default implementations do that for each entity (as long as each enity has an ADao implementation, in the other cases you should provide your own `IClearAllTablesAware` bean). Be cautious here with circular dependencies between entities using foreign keys. You might have to customize `deleteAll()` method of your `ADao`/`IClearAllTablesAware` bean to resolve that before deletion. Check the `persistence.log` if your unit tests seem to hang, it might be filled with warnings regarding circular foreign keys or the test might abort with a `StackOverflowError` because of this.
- **Schema Generation**: per default during testing the schema generation feature of the ORMs is enabled, while in production the schema only gets validated, you can change this behavior with the `de.invesdwin.context.persistence.jpa.PersistenceProperties.DEFAULT_CONNECTION_AUTO_SCHEMA=(VALIDATE|UPDATE|CREATE|CREATE_DROP)` property globally or on a persistence unit basis via `de.invesdwin.context.persistence.jpa.PersistenceUnitContext.CONNECTION_AUTO_SCHEMA@default_pu=(VALIDATE|UPDATE|CREATE|CREATE_DROP)`

### Advanced Customization Example

We had the need to do some advanced database customization for a very large database table which might be interesting as an example here:

-  You can let your DAO implement `IStartupHook` to run some native statements for customization of the automatically generated tables. E.g. the following startup hook changes a MariaDB table to use the TokuDB storage engine and define partitioning for a very large database table:
```java
@Named
@ThreadSafe
public class ConfigurableTradingDayRatingDao extends
        ACustomIdDao<ConfigurableTradingDayRatingEntity, ConfigurableTradingDayRatingEntityId> implements IStartupHook {
        
    ...
        
    @Transactional
    @Override
    public void startup() throws Exception {
        if (PersistenceProperties.getPersistenceUnitContext(getPersistenceUnitName())
                .getConnectionDialect() == ConnectionDialect.MYSQL && !ContextProperties.IS_TEST_ENVIRONMENT
                && isEmpty()) {
            getEntityManager()
                    .createNativeQuery("alter table " + ConfigurableTradingDayRatingEntity.class.getSimpleName()
                            + " ENGINE=TokuDB, COMPRESSION=TOKUDB_LZMA")
                    .executeUpdate();
            getEntityManager()
                    .createNativeQuery("alter table " + ConfigurableTradingDayRatingEntity.class.getSimpleName()
                            + " partition by hash(" + ConfigurableTradingDayRatingEntity_.ratingConfig_id.getName()
                            + ") partitions " + RatingUpdaterProperties.generateExpectedRatingConfigs().size())
                    .executeUpdate();
        }
    }
    
    ...
    
}
```
- Also note that we used a custom Id type for this specifically large table to even spare us the additional long Id column that does not get us anywhere with TokuDB, since it does not support foreign keys anyway. See the following code as an advanced example on how to setup indexes and custom entities:

```java
@Entity
@NotThreadSafe
// unique constraint is modified in later on startup as primary key
@IdClass(ConfigurableTradingDayRatingEntityId.class)
@Table(uniqueConstraints = @UniqueConstraint(columnNames = { "company_id", "ratingConfig_id", "date" }))
// we also need some additional indexes to improve query speeds
@Indexes({ @Index(columnNames = { "date", "ratingConfig_id", "companyCurrentlyStable" }),
    @Index(columnNames = { "company_id", "ratingConfig_id", "date" }),
    @Index(columnNames = { "date", "ratingConfig_id", "companyCurrentlyStableOptimistic" }),
    @Index(columnNames = { "date", "ratingConfig_id", "companyCurrentlyUndervalued" }) })
public class ConfigurableTradingDayRatingEntity extends AValueObject {

    public static class ConfigurableTradingDayRatingEntityId extends AValueObject {
        private Date date;
        private Long company_id;
        private Long ratingConfig_id;

        //demonstrating type conversion here from Date to FDate
        public FDate getDate() {
            return FDate.valueOf(date);
        }

        public void setDate(final FDate date) {
            this.date = FDate.toDate(date);
        }

        public Long getCompany_id() {
            return company_id;
        }

        public void setCompany_id(final Long company_id) {
            this.company_id = company_id;
        }

        public Long getRatingConfig_id() {
            return ratingConfig_id;
        }

        public void setRatingConfig_id(final Long ratingConfig_id) {
            this.ratingConfig_id = ratingConfig_id;
        }

    }

    //workaround for missing foreign keys, the transient instance gets filled by the DAO implementation
    @Transient
    private ConfigurableReportRatingEntity configurableReportRating;
    @Column(nullable = false)
    private Long configurableReportRating_id;

    //denormalized ID fields
    @Id
    @Column(nullable = false)
    @Temporal(TemporalType.DATE)
    private Date date;
    @Id
    @Column(nullable = false)
    private Long company_id;
    @Id
    @Column(nullable = false)
    private Long ratingConfig_id;
    
    ...
    
}
```

## LDAP Modules

These modules provide integration for LDAP clients using [spring-ldap](http://projects.spring.io/spring-ldap/). The following tools are available:

- **ALdapDao**: this is a DAO implementation for LDAP similarly to the ADao available for JPA. Again just extend it for each Entry (this is an Entity in LDAP speak) and write your queries in there. `@Transactional` and QueryDSL support is provided out the box by this module. Configuration is done by the following properties:
```properties
de.invesdwin.context.persistence.ldap.LdapProperties.LDAP_CONTEXT_URI=ldap://localhost:10389
de.invesdwin.context.persistence.ldap.LdapProperties.LDAP_CONTEXT_BASE=dc=invesdwin,dc=de
de.invesdwin.context.persistence.ldap.LdapProperties.LDAP_CONTEXT_USERNAME=uid=admin,ou=system
de.invesdwin.context.persistence.ldap.LdapProperties.LDAP_CONTEXT_PASSWORD=invesdwin
```
- **@DirectoryServerTest**: use this annotation in your unit tests to run an embedded [ApacheDS](http://directory.apache.org/apacheds/) LDAP and Kerberos server. The `DirectoryServer` bean can be injected anywhere and you can load your own LDIF files via it. Alternatively just insert some Entries via the ALdapDao facility. You can also browse the directory with the [Apache Directory Studio](http://directory.apache.org/studio/) client application. To run the embedded directory server inside your production distribution, simply make sure to call  `DirectoryServerContextLocation.activate()` in your Main class before the application bootstrap is started. The integrated Kerberos server follows the configuration provided in the `invesdwin-context-security-kerberos` module:
```properties
de.invesdwin.context.security.kerberos.KerberosProperties.KERBEROS_SERVER_URI=localhost:6088
de.invesdwin.context.security.kerberos.KerberosProperties.KERBEROS_PRIMARY_REALM=INVESDWIN.DE
de.invesdwin.context.security.kerberos.KerberosProperties.KERBEROS_DEBUG=true
de.invesdwin.context.security.kerberos.KerberosProperties.KERBEROS_SERVICE_PRINCIPAL=HTTP/localhost@INVESDWIN.DE
#not needed when KEYTAB is specified; if specified, a default keytab will be generated with information given here
de.invesdwin.context.security.kerberos.KerberosProperties.KERBEROS_SERVICE_PASSPHRASE=invesdwin

# you can give a path to a keytab resoure here alternatively to setting the passphrase; being empty, a default keytab will be generated with principal/passphrase given
de.invesdwin.context.security.kerberos.KerberosProperties.KERBEROS_KEYTAB_RESOURCE=
# instead of generating a new krb5conf according to settings provided here, you can specify a resource to one here; being empty, a default krb5.conf will be generated with the above information
de.invesdwin.context.security.kerberos.KerberosProperties.KERBEROS_KRB5CONF_RESOURCE=
```

## LevelDB Module

The `invesdwin-context-persistence-leveldb` module provides support for the popular [LevelDB](https://github.com/google/leveldb) key-value storage via [JNI](https://github.com/fusesource/leveldbjni). The actual LevelDB API is very low level, so we use [EZDB](https://github.com/criccomini/ezdb) for a bit more comfort. Our module provides the following accessories:

- **ADelegateRangeTable**: this is a wrapper around the RangeTable from EZDB and provides the following benefits:
	- read-write-locking for database synchronization to support multi-threaded access
	- `shouldPurgeTable()` callback to reset the database (e.g. daily or on some condition)
	- improved performance via controlling usage of basic features (you can override the callback methods manually to enable this feature if you can live with the performance penalty, but it should be an explicit decision so they are disabled per default):
		- `allowHasNext()`: catching `NoSuchElementException` instead of checking `iterator.hasNext()` improves iteration speed with LevelDB over large datasets, so this will throw an exception when this is forgotten by the developer
		- `allowPutWithoutBatch()`: using the `newBatch().put`/`delete`/`flush()` mechanism instead of direct put/delete calls improves speed of data manipulation tasks on large data sets, so we throw an exception per default when this is forgotten by the developer
	- `getOrLoad(...)` methods allow you to lazily load missing data via a callback function. E.g. when LevelDB is used as a cache for web requests that download financial data. If it is end-of-day data you might want to refresh them daily thus you could extend `ADailyExpiringDelegateRangeTable` which implements `shouldPurgeTable()` to reset the database daily. The next call to `getOrLoad(...)` would thus download up-to-date financial data daily using your callback function.
	- the table does some sanity checks during initialization and purges the table if that failed so you can reload the data from a web service. E.g. when you changed the serialization structure of the data and the client caches need to be refreshed, this automates the process (beware that this is not storage meant for permanent data, it is rather a solution for caching here, or else this feature would need to be disabled for specific use cases)
	- `ExtendedTypeDelegateSerde` as the default serializer/deserializer with support for most common types with a fallback to [FST](https://github.com/RuedigerMoeller/fast-serialization) for complex types. To get faster serialization/deserialization you should consider providing your own `Serde` implementation via the `newHashKeySerde`/`newRangeKeySerde`/`newValueSerde()` callbacks. Utilizing `ByteBuffer` for custom serialization/deserialization is in most cases the fastest and most compact way, but requires a few lines of manual coding.
- **ATimeSeriesDB**: this is a binary data system that stores continuous time data. It uses LevelDB as an index for the file chunks it saves separately per 10,000 entries which are compressed via [LZ4](https://github.com/jpountz/lz4-java) (which is the fastest and most compact compression algorithm we could determine for our use cases). By chunking the data and looking up the files to process via LevelDB date range lookup, we can provide even faster iteration over large financialdata tick series than would be possible with LevelDB itself. Random access is possible, but you should rather let the in-memory `AGapHistoricalCache` from [invesdwin-util](https://github.com/subes/invesdwin-util#caches) handle that and only use `getLatestValue()` here to hook up the callbacks of that cache. This is because always going to the file storage can be really slow, thus an in-memory cache should be put before it. The raw insert and iteration speed for financial tick data was measured to be about 13 times faster with `ATimeSeriesDB` in comparison to directly using LevelDB (both with performance tuned serializer/deserializer implementations). We were able to process more than 2,000,000 ticks per second with this setup instead of just around 150,000 ticks per second with only LevelDB in place. A more synthetic performance test with simpler data structures (which is included in the modules test cases) resulted in the following numbers:
```
      LevelDB     1,000,000    Writes:     100.68/ms  in   9,932 ms
      LevelDB    10,000,000     Reads:     373.15/ms  in  26,799 ms
ATimeSeriesDB     1,000,000    Writes:   3,344.48/ms  in     299 ms  =>  ~33 times faster
ATimeSeriesDB    10,000,000     Reads:  14,204.55/ms  in     704 ms  =>  ~38 times faster
```
- **ATimeSeriesUpdater**: this is a helper class with which one can handle large inserts/updates into an instance of `ATimeSeriesDB`. This handles the creation of separate chunk files and writing them to disk in the most efficient way.
- **SerializingCollection**: this collection implementation is used to store and retrieve each file chunk. It supports two modes of serialization. The default and slower one supports variable length objects by Base64 encoding the serialized bytes and putting a delimiter between each element. The second and faster approach can be enabled by overriding `getFixedLength()` which allows the collection to skip the Base64 encoding and instead just count the bytes to separate each element. Though as this suggests, it only works with fixed length serialization/deserialization which you can provide by overriding the `toBytes`/`fromBytes()` callback methods (which use FST per default). You can also deviate from the default LZ4 compression algorithm by overriding the `newCompressor`/`newDecompressor` callback methods. Despite efficiently storing financial data, this collection can be used to move any kind of data out of memory into a file to preserve precious memory instead of wasting it on metadata that is only rarely used (e.g. during a backtests we can record all sorts of information in a serialized fashion and load it back from file when generating our reports once. This allows us to run more backtests in parallel which would otherwise be limited by tight memory).
- **ISynchronousChannel**: since LevelDB by design supports only one process that accesses the database (even though it does multithreading properly) the developers say one should build their own server frontend around the database to access it from other processes. This makes also sense in order to have only one process that keeps the database updated with expiration checks a single instance of `ATimeSeriesUpdater`. Though this creates the problem of how to build a IPC (Inter-Process-Communication) mechanism that does not slow down the data access too much. As a solution, this module provides a unidirectional channel (you should use one channel for requests and a separate one for responses) for blazing fast IPC on a single computer with the following tools:
	- **Named Pipes**: the classes `PipeSynchronousReader` and `PipeSynchronousWriter` classic implementations for FIFO pipes 		- `SynchronousChannels.createNamedPipe(...)` creates pipes files with the command line tool `mkfifo`/`mknod` on Linux/MacOSX, returns false when it failed to create one. On linux you have to make sure to open each pipe with a reader/writer in the correct order on both ends or else you will get a [deadlock because it blocks](http://stackoverflow.com/questions/2246862/not-able-to-read-from-named-pipe-in-java). The correct way to do this is to first 
		- `SynchronousChannels.isNamedPipeSupported(...)` tells you if those are supported so you can fallback to a different implementation e.g. on Windows (though you could write your own mkfifo.exe/mknod.exe implementation and put it into PATH to make this work with Windows named pipes. At least currently there is no command line tool directly available for this on Windows, despite [makepipe](https://support.microsoft.com/en-us/kb/68941) which might be bundled with some versions of SQL Server, but we did not try to implement this because there is a better alternative to named pipes below that works on any operating system)
	- **Memory Mapping**: the classes `MappedSynchronousReader` and `MappedSynchronousWriter` provide channel implementations that are based on memory mapped files (inspired by [Chronicle-Queue](https://github.com/OpenHFT/Chronicle-Queue) and reusing parts of [MappedBus](https://github.com/caplogic/Mappedbus) while making it even simpler for maximum speed). This implementation has the benefit of being supported on all operating systems and being slightly faster than Named Pipes for the workloads we have tested, but your mileage might vary and you should test it yourself. Anyway you should still keep communication to a minimum, thus making chunks large enough during iteration to reduce synchronization overhead between the processes, for this a bit of fine tuning on your communication layer might be required.
		- for determining where to place the memory mapped files: `SynchronousChannels.getSharedMemoryFolderOrFallback()` gives the `/dev/shm` folder when available or the temp directory as a fallback for e.g. Windows. Using [tmpfs](https://en.wikipedia.org/wiki/Tmpfs) could improve the throughput a bit, since the OS will then not even try to flush the memory mapped file to disk.
	- **ASpinWait**:  the channel implementations are non-blocking by themselves (Named Pipes are normally blocking, but the implementation uses them in a non-blocking way, while Memory Mapping is per default non-blocking). This causes the problem of how one should wait on a reader without causing delays from sleeps or causing CPU load from spinning. This problem is solved by `ASpinWait`. It first spins when things are rolling and falls back to a fast sleep interval when communication has cooled down. The actual timings can be fully customized. To use this class, just override the `isConditionFulfilled()` method by calling `reader.hasNext()`.
	- **Dynamic Client/Server**: you could utilize RMI with its service registry on localhost  (or something similar) to make processes become master/slave dynamically with failover when the master processes exits. Just let each process race to become the master (first one wins) and let all other processes fallback to being slaves and connecting to the master. The RMI service provides mechanisms to setup the synchronous channels (by handing out pipe files) and the communication will then continue faster via your chosen channel implementation (RMI is slower because it uses the default java serialization and the TCP/IP communication causes undesired overhead). When the master process exits, the clients should just race again to get a new master nominated. To also handle clients disappearing, one should implement timeouts via a heartbeat that clients regularly send to the server to detect missing clients and a response timeout on the client so it detects a missing server. This is just for being bullet-proof, the endspoints should will normally notify the other end when they close a channel, but this might fail when a process exits abnormally (see [SIGKILL](https://en.wikipedia.org/wiki/Unix_signal#SIGKILL)).
