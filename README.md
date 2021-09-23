# invesdwin-context-persistence
This project provides persistence modules for the [invesdwin-context](https://github.com/subes/invesdwin-context) module system.

## Maven

Releases and snapshots are deployed to this maven repository:
```
https://invesdwin.de/repo/invesdwin-oss-remote/
```

Dependency declaration:
```xml
<dependency>
	<groupId>de.invesdwin</groupId>
	<artifactId>invesdwin-context-persistence-jpa-hibernate</artifactId>
	<version>1.0.3</version><!---project.version.invesdwin-context-persistence-parent-->
</dependency>
```

## JPA Modules

The `invesdwin-context-persistence-jpa` module provides a way to code against JPA (Java Persistence API) without having to bind yourself to a specific ORM (Object-Relational-Mapping) framework. In theory you could use the same entities you programmed and switch between Hibernate, OpenJPA, EclipseLink, Datanucleus, Kundera and so on. In practice you will find out that not all JPA implementations are equally good. So for now we only have modules available for:
 - [Hibernate](http://hibernate.org/) (see `invesdwin-context-persistence-jpa-hibernate`) as the most mature JPA implementation for relational databases in our opinion
 - [EclipseLink](http://www.eclipse.org/eclipselink/) (see `invesdwin-context-persistence-jpa-eclipselink`) for testing purposes
 - Additionally there currently are experimental modules for [Datanucleus](http://www.datanucleus.org/) (see `invesdwin-context-persistence-jpa-datanucleus`) and [Kundera](https://github.com/impetus-opensource/Kundera) (see `invesdwin-context-persistence-jpa-kundera`) as they provide connectors to NoSQL databases over JPA. These are in experimental stage since they still fail some of our unit tests for JPA compliance or have some other bugs. But they can already be used when you forego advanced JPA features. 

For now these modules provide a proof of concept that `invesdwin-context-persistence-jpa` stays neutral to any JPA implementation specifically and provides the flexibility to give up on extended JPA features. So the rest of this documentation will tell you what you can do with a comfortable JPA implementation like Hibernate and will give you hints on how you can scale down for different requirements. The main JPA module provides the following tools:

### Entities

- **AEntity**: this is the base class for entities that use optimistic locking, timestamps for creation and last update, as well as a simple numerical ID based on a sequence per table (currently only available in the Hibernate module), as this is the best performing strategy for high load systems. There are a few variations available that you can use as alternative base classes for fine tuning:
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
- **JpaRepository**: since spring-data-jpa is used, you can also define query interfaces that extend `JpaRepository` and leverage the magic spring provides. The `invesdwin-context` application bootstrap will generate the spring-xml configuration for all interfaces in the classpath that extend `JpaRepository` (which do not extend `IDao`) automatically for you and make them reference the correct persistence unit for transactions depending on the entity class given in the `JpaRepository` generic type arguments. It will look up the `@PersistenceUnit` in the entity class itself to determine the bean names for the datasource, transaction manager and so on. See the next paragraphs for more details on this automated configuration mechanism.

### Testing and Configuration

- **persistence.log**: per default our transaction manager logs SQL statements into a log file. It adds information about transactions, so you can always troubleshoot your persistence issues properly. If you want to gain some additional performance during production use, just change the log level of `p6spy` to `OFF` in your logback configuration or better remove the dependency to [p6spy:p6spy](https://github.com/p6spy/p6spy).
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
- **Change DB Config**: to use a different database as a test server, just put a file called `/META-INF/env/${USERNAME}.properties` into your classpath. This file allows you to override the modules default configuration depending on your local development environment (for more information and on how to define distribution specific settings, see the [invesdwin-context documentation](https://github.com/subes/invesdwin-context#tools). Just put the following properties with changed values there (and make sure the connection driver is added as a maven dependency and is available in the classpath, you can also deploy an embedded database like this):
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
    - `another_ds` defines the `DataSource` which normally is a [HikariCP](https://github.com/brettwooldridge/HikariCP) connection pool instance that can be used to gain direct JDBC access to the database. If you don't need the performance or just want to debug some database deadlocks, you can add a dependency to [com.mchange:c3p0](http://www.mchange.com/projects/c3p0/) and it will be used instead. c3p0 provides a statement cache (not all databases can be configured for this with HikariCP) and it provides helpful error messages for problems like database deadlocks.
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
## NoSQL Modules

There are a few NoSQL integration available. Most notably:

### invesdwin-context-persistence-ezdb
The `invesdwin-context-persistence-ezdb` module provides support for the popular [LevelDB](https://github.com/pcmind/leveldb) key-value storage. The actual LevelDB API is very low level, so we use [EZDB](https://github.com/criccomini/ezdb) for a bit more comfort. Our module provides the following accessories:

- **ADelegateRangeTable**: this is a wrapper around the RangeTable from EZDB and provides the following benefits:
	- read-write-locking for database synchronization to support multi-threaded access
	- `shouldPurgeTable()` callback to reset the database (e.g. daily or on some condition)
	- improved performance via controlling usage of basic features (you can override the callback methods manually to enable this feature if you can live with the performance penalty, but it should be an explicit decision so they are disabled per default):
		- Catching `NoSuchElementException` instead of checking `iterator.hasNext()` improves iteration speed with LevelDB over large datasets. So `allowHasNext()` will throw an exception (if not overridden) when this is forgotten by the developer.
		- Using the `newBatch().put`/`delete`/`flush()` mechanism instead of direct put/delete calls improves speed of data manipulation tasks on large data sets. Though no exception will be thrown for this misusage.
	- `getOrLoad(...)` methods allow you to lazily load missing data via a callback function. E.g. when LevelDB is used as a cache for web requests that download financial data. If it is end-of-day data you might want to refresh them daily thus you could extend `ADailyExpiringDelegateRangeTable` which implements `shouldPurgeTable()` to reset the database daily. The next call to `getOrLoad(...)` would thus download up-to-date financial data daily using your callback function.
	- the table does some sanity checks during initialization and purges the table if that failed so you can reload the data from a web service. E.g. when you changed the serialization structure of the data and the client caches need to be refreshed, this automates the process (beware that this is not storage meant for permanent data, it is rather a solution for caching here, or else this feature would need to be disabled for specific use cases)
	- `ExtendedTypeDelegateSerde` as the default serializer/deserializer with support for most common types with a fallback to [FST](https://github.com/RuedigerMoeller/fast-serialization) for complex types. To get faster serialization/deserialization you should consider providing your own `Serde` implementation via the `newHashKeySerde`/`newRangeKeySerde`/`newValueSerde()` callbacks. Utilizing `ByteBuffer` for custom serialization/deserialization is in most cases the fastest and most compact way, but requires a few lines of manual coding.

### invesdwin-context-persistence-chronicle/mapdb 
- **APersistentMap/APersistentNavigableMap**: if you need storage of large elements, then LevelDB might not bring the best performance because it will reorganize itself often during insertions which makes it very slow. If you need the ordered values from LevelDB, then you can use it as an index and store your large entries in a [ChronicleMap](https://github.com/OpenHFT/Chronicle-Map) or [MapDB](https://github.com/jankotek/mapdb) with this. It is essentially a persistent map with good performance characteristics. You should consider to compress large values with `FastLZ4CompressionFactory.INSTANCE.maybeWrap(serde)` to reduce the file size at a little cpu cost. Though this should be cancelled out by improved IO speed which is more often the bottleneck. This database performs better than LevelDB for databases with large entries, since no reordering on disk is performed.


## Timeseries Module

This is a custom designed NoSQL database that is optimized for backtesting and live running trading strategies. It supports efficient storage and retrieval of tick data while providing identical performance on granular bar intervals.

### invesdwin-context-persistence-timeseriesdb
- **ATimeSeriesDB**: this is a binary data system that stores continuous time data. It uses LevelDB as an index for the file chunks which it saves separately per 10,000 entries which are compressed via [LZ4](https://github.com/jpountz/lz4-java) (which is the fastest and most compact compression algorithm we could determine for our use cases). By chunking the data and looking up the files to process via LevelDB date range lookup, we can provide even faster iteration over large financialdata tick series than would be possible with LevelDB itself. Random access is possible, but you should rather let the in-memory `AGapHistoricalCache` from [invesdwin-util](https://github.com/subes/invesdwin-util#caches) handle that and only use `getLatestValue()` here to hook up the callbacks of that cache. This is because always going to the file storage can be really slow, thus an in-memory cache should be put before it. The raw insert and iteration speed for financial tick data was measured to be about 13 times faster with `ATimeSeriesDB` in comparison to directly using LevelDB (both with performance tuned serializer/deserializer implementations). We were able to process more than 2,000,000 ticks per second with this setup instead of just around 150,000 ticks per second with only LevelDB in place. A more synthetic performance test with a single thread and simpler data structures (which is included in the modules test cases) resulted in the following numbers:

Old Benchmarks (2016, Core i7-4790K with SSD, Java 8):
```
  LevelDB-JNI           1,000,000    Writes:     100.68/ms  in   9,932 ms
  LevelDB-JNI          10,000,000     Reads:     373.15/ms  in  26,799 ms
ATimeSeriesDB (High)    1,000,000    Writes:   3,344.48/ms  in     299 ms  =>  ~33 times faster (with High Compression)
ATimeSeriesDB (High)   10,000,000     Reads:  14,204.55/ms  in     704 ms  =>  ~38 times faster (with High Compression)
```
New Benchmarks (2021, Core i9-9900k with SSD, Java 16):
```
   ezdb-RocksDB-JNI             Writes (PutBatch):       44.86/ms  => ~80% slower
           CQEngine (Disk)      Writes (PutBatch):       48.79/ms  => ~78.6% slower (uses SQLite internally; expensive I/O and huge size for large databases)
   ezdb-LevelDB-JNI             Writes (PutBatch):       63.33/ms  => ~72% slower
           CQEngine (OffHeap)   Writes (PutBatch):       65.97/ms  => ~71% slower (no persistence)
      ezdb-LMDB-JNR             Writes (PutBatch):      152.05/ms  => ~33% slower
Indeed-RecordLogDirectory       Writes (Append):        199.19/ms  => ~13% slower
              MapDB             Writes (Put):           214.20/ms  => ~6% slower
	  TreeMapDB             Writes (Put):           223.12/ms  => ~2% slower
  ezdb-LevelDB-Java             Writes (PutBatch):      228.07/ms  => using this as baseline
       InfluxDB-1.x             Writes (PutBatch):      252.08/ms  => ~10% faster
     ChronicleQueue             Writes (Append):        442.48/ms  => ~90% faster
       ezdb-LsmTree             Writes (Put):           457.96/ms  => ~94% faster
    Indeed-MphTable             Writes (WriteAll):      491.50/ms  => ~2.15 times as fast (immutable after creation)
      ezdb-BTreeMap             Writes (Put):         1,048.22/ms  => ~4.6 times as fast (no persistence)
           CQEngine (OnHeap)    Writes (PutBatch):    1,083.07/ms  => ~4.7 times as fast (no persistence)
       ChronicleMap (Disk)      Writes (Put):         1,233.05/ms  => ~5.4 times as fast
       ChronicleMap (Memory)    Writes (Put):         1,475.36/ms  => ~6.5 times as fast (no persistence)
       ezdb-TreeMap             Writes (Put):         1,902.77/ms  => ~8.3 times as fast (no persistence)
ezdb-ConcurrentSkipListMap      Writes (Put):         2,358.21/ms  => ~10.3 times as fast (no persistence)
Indeed-BasicRecordFile          Writes (Append):      2,403.85/ms  => ~10.5 times as fast
Indeed-BlockCompRecordFile      Writes (Append):      4,273.32/ms  => ~18.7 times as fast
      ATimeSeriesDB (High)      Writes (Append):      6,449.49/ms  => ~28.3 times as fast (High Compression)
      ATimeSeriesDB (Fast)      Writes (Append):     26,080.38/ms  => ~114.3 times as fast (Fast Compression)
      ATimeSeriesDB (None)      Writes (Append):     34,069.23/ms  => ~149.4 times as fast (Disabled Compression; 2x Size of ATimeSeriesDB with Compression)
            QuestDB             Writes (Append):     36,191.23/ms  => ~158.7 times as fast (tested on Java 11; 4x Size of ATimeSeriesDB with Compression)

            QuestDB              Reads (Get):            49.64/ms  => ~80% slower
   ezdb-RocksDB-JNI              Reads (Get):            58.71/ms  => ~76% slower
   ezdb-LevelDB-JNI              Reads (Get):            81.00/ms  => ~67% slower
      ezdb-LMDB-JNR              Reads (Get):           186.07/ms  => ~24% slower
  ezdb-LevelDB-Java              Reads (Get):           244.29/ms  => using this as baseline
          TreeMapDB              Reads (Get):           627.79/ms  => ~2.6 times as fast
	      MapDB              Reads (Get):         1,220.11/ms  => ~5 times as fast
       ezdb-LsmTree              Reads (Get):         1,846.59/ms  => ~7.6 times as fast
ezdb-ConcurrentSkipListMap       Reads (Get):         2,695.42/ms  => ~11 times as fast
    Indeed-MphTable              Reads (Get):         2,773.93/ms  => ~11.35 times as fast
       ChronicleMap (Disk)       Reads (Get):         2,836.80/ms  => ~11.6 times as fast
      ezdb-BTreeMap              Reads (Get):         2,908.67/ms  => ~11.9 times as fast
       ChronicleMap (Memory)     Reads (Get):         3,037.94/ms  => ~12.4 times as fast
       ezdb-TreeMap              Reads (Get):         4,875.67/ms  => ~20 times as fast
           CQEngine (Disk)       Reads (Get):         5,656.11/ms  => ~23.15 times as fast
           CQEngine (OnHeap)     Reads (Get):         5,732.63/ms  => ~23.5 times as fast
           CQEngine (OffHeap)    Reads (Get):         5,813.95/ms  => ~23.8 times as fast
       
           CQEngine (Disk)       Reads (GetLatest):       5.59/ms  => ~96.7% slower (using "ORDER BY key DESC")
            QuestDB              Reads (GetLatest):      23.63/ms  => ~86% slower (using "SELECT max(key)"; "ORDER BY key DESC" results in ~91/s which is 99.95% slower)
   ezdb-RocksDB-JNI              Reads (GetLatest):      56.12/ms  => ~66.5% slower
   ezdb-LevelDB-JNI              Reads (GetLatest):      72.34/ms  => ~57% slower
      ezdb-LMDB-JNR              Reads (GetLatest):     150.01/ms  => ~10% slower
  ezdb-LevelDB-Java              Reads (GetLatest):     167.48/ms  => using this as baseline
      ATimeSeriesDB              Reads (GetLatest):     286.21/ms  => ~71% faster (after initialization, uses ChronicleMap as lazy index)
           CQEngine (OffHeap)    Reads (GetLatest):     300.70/ms  => ~80% faster (using "ORDER BY key DESC")
           CQEngine (OnHeap)     Reads (GetLatest):     314.20/ms  => ~90% faster (using "ORDER BY key DESC")
	  TreeMapDB              Reads (GetLatest):     586.34/ms  => ~3.5 times as fast
ezdb-ConcurrentSkipListMap       Reads (GetLatest):     985.03/ms  => ~5.9 times as fast
       ezdb-LsmTree              Reads (GetLatest):   1,202.41/ms  => ~7.2 times as fast
      ezdb-BTreeMap              Reads (GetLatest):   2,783.96/ms  => ~16.6 times as fast
       ezdb-TreeMap              Reads (GetLatest):   3,235.20/ms  => ~19.3 times as fast
       
           CQEngine (Disk)       Reads (Iterator):      224.01/ms  => ~89.5% slower (using "Query All")
           CQEngine (OffHeap)    Reads (Iterator):      230.05/ms  => ~89.2% slower (using "Query All")
   ezdb-LevelDB-JNI              Reads (Iterator):      327.20/ms  => ~84.6% slower
              MapDB              Reads (Iterator):      351.85/ms  => ~83.4% slower (unordered)
       InfluxDB-1.x              Reads (Iterator):      649.31/ms  => ~69.4% slower
   ezdb-RocksDB-JNI              Reads (Iterator):      672.35/ms  => ~68.7% slower
       ezdb-LsmTree              Reads (Iterator):      879.74/ms  => ~58.6% slower
  ezdb-LevelDB-Java              Reads (Iterator):    2,125.29/ms  => using this as baseline
           CQEngine (OnHeap)     Reads (Iterator):    3,345.38/ms  => ~60% faster (using "Query All")
Indeed-BasicRecordFile           Reads (Iterator):    4,735.45/ms  => ~2.2 times as fast
       ChronicleMap (Disk)       Reads (Iterator):    5,816.66/ms  => ~2.7 times as fast (unordered)
       ChronicleMap (Memory)     Reads (Iterator):    6,766.36/ms  => ~3.2 times as fast (unordered)
Indeed-RecordLogDirectory        Reads (Iterator):    8,701.71/ms  => ~4.1 times as fast
Indeed-BlockCompRecordFile       Reads (Iterator):    9,285.14/ms  => ~4.37 times as fast
      ezdb-LMDB-JNR              Reads (Iterator):    9,330.80/ms  => ~4.4 times as fast
     ChronicleQueue              Reads (Iterator):   16,583.75/ms  => ~7.8 times as fast
          TreeMapDB              Reads (Iterator):   21,551.72/ms  => ~10.1 times as fast
       ezdb-TreeMap              Reads (Iterator):   27,463.10/ms  => ~12.9 times as fast
      ezdb-BTreeMap              Reads (Iterator):   30,313.13/ms  => ~14.3 times as fast
      ATimeSeriesDB (Fast)       Reads (Iterator):   31,920.33/ms  => ~15 times as fast (no caching)
      ATimeSeriesDB (High)       Reads (Iterator):   32,148.14/ms  => ~15.1 times as fast (no caching)
ezdb-ConcurrentSkipListMap       Reads (Iterator):   32,629.62/ms  => ~15.35 times as fast
    Indeed-MphTable              Reads (Iterator):   34,411.56/ms  => ~16.2 times as fast (unordered)
      ATimeSeriesDB (None)       Reads (Iterator):   34,727.05/ms  => ~16.3 times as fast (no caching)
      ATimeSeriesDB (Cached)     Reads (Iterator):   97,484.89/ms  => ~45.9 times as fast (internal FileBufferCache keeps hot segments in memory)
            QuestDB              Reads (Iterator):  132,910.54/ms  => ~62.5 times as fast (nice! cached buffers with flyweight pattern)
```
- **ATimeSeriesUpdater**: this is a helper class with which one can handle large inserts/updates into an instance of `ATimeSeriesDB`. This handles the creation of separate chunk files and writing them to disk in the most efficient way.
- **SerializingCollection**: this collection implementation is used to store and retrieve each file chunk. It supports two modes of serialization. The default and slower one supports variable length objects by Base64 encoding the serialized bytes and putting a delimiter between each element. The second and faster approach can be enabled by overriding `getFixedLength()` which allows the collection to skip the Base64 encoding and instead just count the bytes to separate each element. Though as this suggests, it only works with fixed length serialization/deserialization which you can provide by overriding the `toBytes`/`fromBytes()` callback methods (which use FST per default). You can also deviate from the default LZ4 high compression algorithm by overriding the `newCompressor`/`newDecompressor` callback methods. Despite efficiently storing financial data, this collection can be used to move any kind of data out of memory into a file to preserve precious memory instead of wasting it on metadata that is only rarely used (e.g. during a backtests we can record all sorts of information in a serialized fashion and load it back from file when generating our reports once. This allows us to run more backtests in parallel which would otherwise be limited by tight memory).
- **ASegmentedTimeSeriesDB**: if it is undesirable to always have the whole time series updated inside the `ATimeSeriesDB`, use this class to split it into segments. You provide an algorithm with which the segment time ranges can be calculated (e.g. monthly via `PeriodicalSegmentFinder.newCache(new Duration(1, FTimeUnit.MONTHS))`) and define the limits of your time series (e.g. from 2001-01-01 to 2018-01-01). The database will request the data for the individual segments when they are first needed. Thus the `ATimeSeriesUpdater` is handled by the database itself. This is helpful when you only need a few parts of the series and require those parts relatively fast without the overhead of calculating segments before or after. For example when calculating bars from ticks for a chart that shows only a small time range. Be aware that segments are immutable once they are created. If you need support for incomplete segments, look at `ALiveSegmentedTimeSeriesDB` next.
- **ALiveSegmentedTimeSeriesDB**: this is the same as the `ASegmentedTimeSeriesDB` with the addition that it supports collecting elements into an incomplete segment before persisting it into an immutable state. The so called live segment is always the latest available segment (e.g. with above monthly segments from 2018-01-01 to 2018-02-01) after the defined limits for the historical segments (which was from 2001-01-01 to 2018-01-01). The historical limits define where the segments are allowed to be initialized via pulling the data for the segments as normally done by `ASegmentedTimeSeriesDB`. The live segment is filled initially and updated via pushing values by calling `putNextLiveValue(...)` in the proper order of arrival. As soon as a value arrives that marks another segment beyond the current live one, the live segment gets persisted as a historical segment and a new live segment gets collected. You just have to make sure that you add the live values without any missing gaps and properly calculate the historical limits with the arrival of new segments. The rest is handled by the database. This is helpful when you still want to make the latest calculated bars available inside a chart without the segment being complete in real time. Once the live values are added, the chart can request further data. Though be aware that an element that was added to the live segment can not be replaced later. Thus an in-progress bar that is updated with each tick needs to be handled from the outside. Only complete bars are allowed to be added to the live segment.

In order to share data between processes and properly coordinate handling of updates have a look at [Synchronous Channels](https://github.com/invesdwin/invesdwin-context-integration/blob/master/README.md#synchronous-channels). They can also provide a way to create manageable and composable data pipelines between threads, processes and networks. The idea is to build high level services around this storage instead of exposing the data storage itself as a service. This way performance can be maximized based on business requirements instead of protocol requirements.

## Support

If you need further assistance or have some ideas for improvements and don't want to create an issue here on github, feel free to start a discussion in our [invesdwin-platform](https://groups.google.com/forum/#!forum/invesdwin-platform) mailing list.
