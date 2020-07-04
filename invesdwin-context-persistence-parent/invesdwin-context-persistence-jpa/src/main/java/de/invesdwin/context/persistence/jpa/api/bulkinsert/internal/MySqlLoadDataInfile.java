package de.invesdwin.context.persistence.jpa.api.bulkinsert.internal;

import java.lang.reflect.Field;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Set;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import javax.persistence.EntityManager;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EntityType;
import javax.sql.DataSource;

import org.apache.commons.io.IOUtils;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import de.invesdwin.context.persistence.jpa.PersistenceUnitContext;
import de.invesdwin.context.persistence.jpa.api.dao.entity.IEntity;
import de.invesdwin.context.persistence.jpa.api.util.Attributes;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.lang.reflection.Reflections;
import de.invesdwin.util.time.fdate.FDate;

@ThreadSafe
public class MySqlLoadDataInfile<E> implements IBulkInsertEntities<E> {

    private final Class<?> genericType;
    @GuardedBy("persistLock")
    private final PersistenceUnitContext puContext;
    @GuardedBy("persistLock")
    private final DataSource ds;
    private final EntityManager em;
    private final List<String> javaColumnNames;
    private boolean disabledChecks;
    private boolean skipPrepareEntities;
    private final Object persistLock = new Object();
    @GuardedBy("stageLock")
    private StringBuilder curFileAndOutputStream;
    private final Object stageLock = new Object();

    public MySqlLoadDataInfile(final Class<E> genericType, final PersistenceUnitContext puContext) {
        this.genericType = genericType;
        this.puContext = puContext;
        this.ds = puContext.getDataSource();
        this.em = puContext.getEntityManager();
        this.javaColumnNames = determineJavaColumnNames(puContext);
    }

    /**
     * set unique_checks = 0; set foreign_key_checks = 0; set sql_log_bin=0;
     */
    @Override
    public MySqlLoadDataInfile<E> withDisabledChecks(final boolean disabledChecks) {
        this.disabledChecks = disabledChecks;
        return this;
    }

    @Override
    public boolean isDisabledChecks() {
        return disabledChecks;
    }

    @Override
    public boolean isSkipPrepareEntities() {
        return skipPrepareEntities;
    }

    @Override
    public IBulkInsertEntities<E> withSkipPrepareEntities(final boolean skipPrepareEntities) {
        this.skipPrepareEntities = skipPrepareEntities;
        return this;
    }

    @SuppressWarnings({ "unchecked", "GuardedBy" })
    private String createQuery(final String workFile) {
        final StringBuilder sb = new StringBuilder();
        sb.append("LOAD DATA LOCAL INFILE '");
        sb.append(workFile);
        sb.append("' INTO TABLE ");
        sb.append(genericType.getSimpleName());
        sb.append(" ");
        sb.append("FIELDS TERMINATED BY ',' ");
        sb.append("ENCLOSED BY '\"'  ");
        sb.append("ESCAPED BY '\\\\' ");
        sb.append("LINES TERMINATED BY '\\n' ");
        sb.append("(");
        final EntityManager em = puContext.getEntityManager();
        final EntityType<E> et = (EntityType<E>) em.getMetamodel().entity(genericType);
        final Set<Attribute<? super E, ?>> attrs = et.getAttributes();
        boolean first = true;
        final List<String> booleanColumns = new ArrayList<String>();
        for (final Attribute<? super E, ?> attr : attrs) {
            if (skipColumn(attr.getJavaMember().getName())) {
                continue;
            }
            if (!first) {
                sb.append(", ");
            }
            first = false;
            final String sqlName = Attributes.extractNativeSqlColumnName(attr);
            //handling boolean properly http://stackoverflow.com/questions/15683809/load-data-from-csv-inside-bit-field-in-mysql
            if (Reflections.isBoolean(attr.getJavaType())) {
                sb.append("@");
                booleanColumns.add(sqlName);
            }
            sb.append(sqlName);
        }
        sb.append(")");
        if (!booleanColumns.isEmpty()) {
            //      set valore=cast(@valore as signed);
            sb.append(" set ");
            boolean firstBooleanColumn = true;
            for (final String booleanColumn : booleanColumns) {
                if (!firstBooleanColumn) {
                    sb.append(",");
                }
                firstBooleanColumn = false;
                sb.append(booleanColumn);
                sb.append("=cast(@");
                sb.append(booleanColumn);
                sb.append(" as signed)");
            }
        }

        return sb.toString();
    }

    @SuppressWarnings("unchecked")
    private List<String> determineJavaColumnNames(final PersistenceUnitContext puContext) {
        final List<String> javaColumnNames = new ArrayList<String>();
        final EntityManager em = puContext.getEntityManager();
        final EntityType<E> et = (EntityType<E>) em.getMetamodel().entity(genericType);
        final Set<Attribute<? super E, ?>> attrs = et.getAttributes();
        for (final Attribute<? super E, ?> attr : attrs) {
            final String javaName = attr.getJavaMember().getName();
            javaColumnNames.add(javaName);
        }
        return javaColumnNames;
    }

    @Override
    public void stage(final List<E> entities) {
        if (!skipPrepareEntities) {
            prepareEntities(entities);
        }
        stageEntities(entities);
    }

    @Transactional(propagation = Propagation.NEVER)
    private void stageEntities(final List<E> entities) {
        for (final E entity : entities) {
            stage(entity);
        }
    }

    @Transactional(readOnly = true, propagation = Propagation.SUPPORTS)
    private void prepareEntities(final List<E> entities) {
        Assertions.checkFalse(TransactionSynchronizationManager.isActualTransactionActive());
        //fake a transaction to prevent TransactionRequiredException in SharedEntityManager Proxy
        TransactionSynchronizationManager.setActualTransactionActive(true);
        try {
            for (final E entity : entities) {
                em.persist(entity);
                em.remove(entity);
            }
        } finally {
            TransactionSynchronizationManager.setActualTransactionActive(false);
        }
    }

    private void stage(final E entity) {
        final StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (final String javaColumnName : javaColumnNames) {
            if (skipColumn(javaColumnName)) {
                continue;
            }
            if (!first) {
                sb.append(",");
            }
            first = false;
            final Field f = Reflections.findField(genericType, javaColumnName);
            Reflections.makeAccessible(f);
            Object value = Reflections.getField(f, entity);
            if (value instanceof IEntity) {
                final IEntity valueEntity = (IEntity) value;
                value = valueEntity.getId();
            } else if (value instanceof Date) {
                value = FDate.valueOf((Date) value).toString(FDate.FORMAT_ISO_DATE_TIME_SPACE);
            } else if (value instanceof Calendar) {
                value = FDate.valueOf((Calendar) value).toString(FDate.FORMAT_ISO_DATE_TIME_SPACE);
            } else if (value instanceof Boolean) {
                final Boolean bValue = (Boolean) value;
                if (bValue) {
                    value = 1;
                } else {
                    value = 0;
                }
            }
            final String valueStr = String.valueOf(value).replace("\"", "\\\\\"");
            sb.append("\"");
            sb.append(valueStr);
            sb.append("\"");
        }
        sb.append("\n");
        synchronized (stageLock) {
            if (curFileAndOutputStream == null) {
                curFileAndOutputStream = new StringBuilder();
            }
            curFileAndOutputStream.append(sb.toString());
        }
    }

    private boolean skipColumn(final String javaColumnName) {
        /*
         * autoincrement values should be skipped in the import:
         * http://stackoverflow.com/questions/14083709/mysql-load-data-infile-auto-incremented-id-value
         */
        if (IEntity.class.isAssignableFrom(genericType) && IEntity.ID_COLUMN_NAME.equals(javaColumnName)) {
            final Field field = Reflections.findField(genericType, javaColumnName);
            final GeneratedValue annotation = Reflections.getAnnotation(field, GeneratedValue.class);
            if (annotation.strategy() == GenerationType.IDENTITY || annotation.strategy() == GenerationType.AUTO) {
                return true;
            }
        }
        return false;
    }

    @Override
    public int persist() {
        final StringBuilder workFileAndOutputStream;
        synchronized (stageLock) {
            workFileAndOutputStream = curFileAndOutputStream;
            curFileAndOutputStream = null;
        }
        if (workFileAndOutputStream == null) {
            return 0;
        }
        if (workFileAndOutputStream.length() == 0) {
            return 0;
        }
        synchronized (persistLock) {
            try {
                return internalPersist(workFileAndOutputStream);
            } catch (final Throwable e) {
                //restore file contents to be able to resume later if stage is not called in same method as persist
                synchronized (stageLock) {
                    if (curFileAndOutputStream == null || curFileAndOutputStream.length() == 0) {
                        curFileAndOutputStream = workFileAndOutputStream;
                    } else {
                        curFileAndOutputStream.append(workFileAndOutputStream);
                    }
                }
                throw new RuntimeException("On " + genericType.getSimpleName(), e);
            }
        }
    }

    @SuppressWarnings("GuardedBy")
    @Transactional
    private int internalPersist(final StringBuilder workFileAndOutputStream) throws SQLException {
        try (Connection conn = ds.getConnection()) {
            try (com.mysql.jdbc.Statement stmt = conn.createStatement().unwrap(com.mysql.jdbc.Statement.class)) {
                final boolean prevAutoCommit = conn.getAutoCommit();
                if (disabledChecks) {
                    if (prevAutoCommit) {
                        stmt.execute("set autocommit=0");
                    }
                    stmt.execute("set unique_checks=0");
                    stmt.execute("set foreign_key_checks=0");
                }
                final String query = createQuery("memoryFile.txt");
                //http://jeffrick.com/2010/03/23/bulk-insert-into-a-mysql-database/
                stmt.setLocalInfileInputStream(
                        IOUtils.toInputStream(workFileAndOutputStream.toString(), Charset.defaultCharset()));
                final int countUpdated = stmt.executeUpdate(query);
                final SQLWarning warnings = conn.getWarnings();
                if (disabledChecks) {
                    stmt.execute("set foreign_key_checks=1");
                    stmt.execute("set unique_checks=1");
                    if (prevAutoCommit) {
                        stmt.execute("set autocommit=1");
                    }
                }
                if (warnings != null) {
                    throw warnings;
                }
                return countUpdated;
            }
        }
    }

    @Override
    public void close() {
        synchronized (stageLock) {
            curFileAndOutputStream = null;
        }
    }

}
