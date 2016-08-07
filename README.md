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

## JPA

The `invesdwin-context-persistence-jpa` module provides a way to code against JPA without having to bind yourself to a specific ORM (Object-Relational-Mapping) framework. In theory you could use the same entities you programmed and switch between Hibernate, OpenJPA, EclipseLink, Datanucleus, Kundera and so on. In practice you will find out that not all JPA implementations are equally good. So for now we only have modules available for [Hibernate](http://hibernate.org/) (see `invesdwin-context-persistence-jpa-hibernate`) as the most mature JPA implementation for relational databases in our opinion. Additionally there currently are experimental modules for [Datanucleus](http://www.datanucleus.org/) (see `invesdwin-context-persistence-jpa-datanucleus`) and [Kundera](https://github.com/impetus-opensource/Kundera) (see `invesdwin-context-persistence-jpa-kundera`) as they provide connectors to NoSQL databases over JPA. These are in experimental stage since they still fail some of our unit tests for JPA compliance or have some other bugs. But they can already be used when you forego advanced JPA features. For now they will have to suffice as a proof of concept that `invesdwin-context-persistence-jpa` stays neutral to any JPA implementation specifically and provides the flexibility to give up on extended JPA features. So the rest of this documentation will tell you what you can do with a full JPA implementation like Hibernate and will give you hints on how you can scale down for different requirements. The main JPA module provides the following tools:

### Entities

- **AEntity**: this is the base class class for entities that use optimistic locking, timestamps for creation and last update, as well as a simple numerical ID based on a sequence per table (currently only available in the hibernate module), as this is the best performing variation for high load systems. There are a few variations available that you can use as alternative base classes for fine tuning:
  - `AEntityWithIdentity`: using the identity column feature of your database if supported as the ID generator strategy
  - `AEntityWithTableSequence`: using a table for sequences which is supported by any database
  - `AEntityWithSequence`: using the high performance variation of one sequence per table in Hibernate, or the default sequence strategy on other ORMs (this is the default base class for `AEntity`)
- **AUnversionedEntity**: this is the base class that only implements the ID column itself, without adding optimistic locking or timestamps. It is useful when you have a very large table where you want spare you the hard disk space for uneccessary additional fields on millions of rows. As above you have the option to use the other ID generator strategies by extending `AUnversionedEntityWithIdentity` or `AUnversionedEntityWithTableSequence`, while `AUnversionedEntityWithSequence` is the default.

### Data Access Objects (DAO)

- **ADao**: this is the default DAO implementation. It is always bound to provide queries and so on for one specific entity.  It is the default implementation for a numerical ID as used in AEntity. The DAO provides support for [spring-data-jpa](http://projects.spring.io/spring-data-jpa/) and [QueryDSL](http://www.querydsl.com/) while extending it with convenience methods for [Query by Example](https://en.wikipedia.org/wiki/Query_by_Example). To get the most out of your DAOs, your entities should follow the following assumptions:
  - only use basic Java object types as fields in your entities and convert to/from a different desired type in the getters and setters. E.g. use a Date/BigDecimal fields and convert to/from FDate/Decimal (see [invesdwin-util](https://github.com/subes/invesdwin-util)) in the getter/setter methods. Also make sure you do not use primitive data types such as long, double but instead object types like Long, Double as fields, so that the null checks can work properly in validation phase and for Query by Example convenience.
  - utilize BeanValidation annotations extensively to only allow clean data into your database
  - put the invesdwin @Indexes annotation on your entities to let the framework generate them for you (since the JPA standard is missing this)
  - if you want to roll your own customized entities, just do that and implement the `IEntity` interface to still be able to use our ADao base classe, which needs to have a way to retrieve the numerical IDs.
- **ACustomIdDao**: this DAO implementation can be used when you want to use something different as an ID in combination with your custom entity that not necessarily has a numeric ID (otherwise you could use `ADao` with `IEntity`). E.g. when you want to use a composite primary key as the ID.
- **ARepository**: for a good design, DAOs should only contain queries for their specific entity. If you want to write queries that span multiple entities, you should rather externalize them into a repository implementation, which can also reference multiple DAOs to get its work done. Or you can write a repository for a special task like complex searching over different entities that requires a whole bunch of code to write the queries. Regarding queries here are a few tips:
 - favor QueryDSL over JPQL for complex queries, since that has the advantage of being type safe and thus robust against refactorings in your entities (the `Q...Entity` classes get generated via an annotation processor and will give compile errors for queries you have to adjust)
  - use Query by Example for 80% of your simple query needs
  - leverage the query cache and JPA Level 2 cache where appropriate instead of setting up your own cache where possible (per default backed by EhCache in the Hibernate module)
  - use the `BulkInsertEntities` class to push bulk insert speeds to the maximum. It leverages the LOAD DATA INFILE on a MySQL database or at least does proper batch inserts for other databases (which is also tricky to get right sometimes).
  - put the spring `@Transactional` annotation around methods that do DML operations on the database, since per default only read only transactional attributes are applied. You should also put `@Transactional` annotations around methods in your services/beans that do database operations over multiple repositories and DAOs, so that all operations share the same transaction and get roll backed together. Also don't worry about multiple persistence units here, since the `ContextDelegatingTransactionManager` takes care of shared transactions for you. Since invesdwin-context is using AspectJ you can even put `@Transactional` inside your `@Configurable` non-bean objects that you instantiate yourself.
- **JpaRepository**: since spring-data-jpa is used, you can also define query interfaces that extend `JpaRepository` and leverage the magic spring provides. The `invesdwin-context` application bootstrap will generate the spring-xml configuration for all interfaces in the classpath that extend `JpaRepository` (which do not extend `IDao`) automatically for you and make them reference the correct persistence unit for transactions depending on the entity class given in the `JpaRepository` generic type arguments. It will look up the `@PersistenceUnit` in the entity class itself to determine the bean names for the datasource, transaction manager and so on. See the next paragraphs for the more details on this automated configuration mechanism.

### Testing and Configuration

- **persistence.log**: per default our transaction manager logs SQL statements into a log file. It adds information about transactions, so you can always troubleshoot your persistence issues properly. If you want to gain some additional performance during production use, just change the log level of `org.jdbcdslog.StatementLogger` to `OFF` in your logback configuration.
- **@PersistenceTest**: per default all JUnit tests run in an in-memory H2 database. Add this annotation to your test case to switch to a real server during testing (e.g. when you want to test against real data you seeded during development of a website). The default server is supposed to be a local MySQL instance that is setup with the following script:
```sql
USE mysql;
CREATE USER 'invesdwin'@'localhost' IDENTIFIED BY 'invesdwin';
GRANT ALL ON invesdwin.* TO 'invesdwin'@'localhost';
GRANT SUPER ON *.* TO 'invesdwin'@'localhost';
CREATE DATABASE invesdwin;
```
- To install a MySQL instance on ubuntu, use the following commands:
```bash
sudo apt-get install mysql-server mysql-workbench
# increase some limits
sudo sed -i s/max_connections.*/max_connections=1000/ /etc/mysql/my.cnf
echo "innodb_file_format=Barracuda" | sudo tee -a /etc/mysql/my.cnf
sudo /etc/init.d/mysql restart
```
- To use a different database as a test server, just put a file called `/META-INF/env/${USERNAME}.properties` into your classpath. This file allows you to override the modules default configuration depending on your local development environment (for more information and on how to define distribution specific settings, see the [invesdwin-context documentation](https://github.com/subes/invesdwin-context). Just put following properties with changed values there (and make sure the connection driver is added as a maven dependency and available in the classpath):
```properties
de.invesdwin.context.persistence.jpa.PersistenceUnitContext.CONNECTION_DRIVER@default_pu=com.mysql.jdbc.Driver
de.invesdwin.context.persistence.jpa.PersistenceUnitContext.CONNECTION_URL@default_pu=jdbc:mysql://localhost:3306/invesdwin
de.invesdwin.context.persistence.jpa.PersistenceUnitContext.CONNECTION_USER@default_pu=invesdwin
de.invesdwin.context.persistence.jpa.PersistenceUnitContext.CONNECTION_PASSWORD@default_pu=invesdwin
de.invesdwin.context.persistence.jpa.PersistenceUnitContext.CONNECTION_DIALECT@default_pu=MYSQL
```
- Notice the `@default_pu` string in the properties? This defines the persistence unit this configuration is for. Duplicating these values and changing the name of the persistence unit allows you to setup multiple databases to be used in one application. Depending on the dialect and which persistence provider modules you have in your classpath (e.g. Hibernate, Datanucleus, Kundera) you can in theory even mix Hibernate for relational databases, Datanucleus for HBase and Kundera for Cassandra as multiple ORMs in the same application. Though we still have to define a few more `invesdwin-persistence-jpa-*` modules and create an actual proof of concept for this feature, but it is an interesting idea regarding polyglot persistence. For now this just allows you to e.g. use multiple databases with the Hibernate module.
  - you can annotate your entity class with `@PersistenceUnit("another_pu")` to tell the invesdwin-context bootstrap to associate this entity with the specific persistence unit configuration
  - the `ACustomIdDao`/`ADao` class will lookup its entity class to determine the persistence unit (see `PersistenceProperties` class) to be used for its entity manager and transactions. Since `ARepository` does not have an associated entity, you have to provide the proper persistence unit name by overriding the `getPersistenceUnitName()` method. 
  - the `IPersistenceUnitAware` interface defines this method and `ARepository` implements this interface. You can actually also implement this interface inside any of your spring Beans/Services to add this information. It tells the `ContextDelegatingTransactionManager` to use this specific persistence unit for the transactions defined inside this class
  - additionally you can override/specify the persistence unit to use directly with a `@Transactional(value="another_tm")` annotation. This defines a specific transaction manager bean to be used for this transaction definition. It leverages the naming convention of all persistence unit related beans following a specific naming pattern:
    - `another_pu` defines the persistence unit name `another` (`default_pu` being the `default` persistence unit) and is required to always carry the suffix `_pu` so it can be identified as such
    - `another_tm` defines the transaction manager bean and can be used in `@Transactional` annotations as above or to lookup the specific `PlatformTransactionManager` bean to do manual transaction handling by calling its `getTransaction`/`commit`/`rollback` methods
    - `another_emf` defines the `EntityManagerFactory` bean with which you can retrieve `EntityManager` instances. You can make them threadsafe with springs `SharedEntityManagerCreator` or directly use `PersistenceProperties.getPersistenceUnitContext("another_pu").getEntityManager()` to get a thread safe instance.
    - `another_ds` defines the `DataSource` which normally is a [c3p0](http://www.mchange.com/projects/c3p0/) connection pool instance that can be used to gain direct JDBC access to the database. for now c3p0 is the most robust connection pool we found, even if it is not said to be the fastest. At least it provides a `PreparedStatement` cache (which is missing in HikariCP and hard to configure sometimes depending on the JDBC driver), does not give unexplainable runtime exceptions in production environments (we had some encounters of the weird kind with BoneCP) and provides helpful errors when it encounters transaction deadlocks (which can be very helpful during development and troubleshooting of production problems). In most cases we value development comfort higher than raw speed (which definitely comes at a cost), so that is why we chose c3p0 in the end. If you want to use a different connection pool we could provide some means to allow you to override the connection pool factory, but for now it cannot be changed since we did not have the use case for this feature yet (we normally only add variability into the modules as soon/late as we need it).
- per default during testing the schema generation feature of the ORMs is enabled, while in production the schema only gets validated, you can change this behavior with the `de.invesdwin.context.persistence.jpa.PersistenceProperties.DEFAULT_CONNECTION_AUTO_SCHEMA=(VALIDATE|UPDATE|CREATE|CREATE_DROP)` property globally or on a persistence unit basis via `de.invesdwin.context.persistence.jpa.PersistenceUnitContext.CONNECTION_AUTO_SCHEMA@default_pu=(VALIDATE|UPDATE|CREATE|CREATE_DROP)`

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
- Also note that we used a custom Id type for this specifically large table to even spare us the additional long Id column that does not get us anywhere with TokuDB, since it does not support foreign keys anyway. See the following code as an advanced example on how to setup indices and custom entities:

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

## LDAP

## LevelDB
