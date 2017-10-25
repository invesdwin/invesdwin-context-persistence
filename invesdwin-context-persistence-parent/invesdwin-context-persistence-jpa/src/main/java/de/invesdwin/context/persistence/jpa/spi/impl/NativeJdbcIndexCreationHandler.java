package de.invesdwin.context.persistence.jpa.spi.impl;

import javax.annotation.concurrent.NotThreadSafe;
import javax.persistence.EntityManager;
import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.ManagedType;

import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import de.invesdwin.context.beans.hook.IStartupHook;
import de.invesdwin.context.beans.hook.StartupHookManager;
import de.invesdwin.context.persistence.jpa.PersistenceUnitContext;
import de.invesdwin.context.persistence.jpa.api.index.Index;
import de.invesdwin.context.persistence.jpa.api.index.Indexes;
import de.invesdwin.context.persistence.jpa.api.util.Attributes;
import de.invesdwin.context.persistence.jpa.api.util.SqlErr;
import de.invesdwin.context.persistence.jpa.spi.IIndexCreationHandler;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.lang.Strings;
import de.invesdwin.util.lang.UniqueNameGenerator;

@NotThreadSafe
public class NativeJdbcIndexCreationHandler implements IIndexCreationHandler {

    @Transactional(propagation = Propagation.NEVER)
    @Override
    public void createIndexes(final PersistenceUnitContext context, final Class<?> entityClass, final Indexes indexes) {
        final UniqueNameGenerator uniqueNameGenerator = new UniqueNameGenerator();
        for (final Index index : indexes.value()) {
            /*
             * need this to run even after the startup occured, since tests might discover new entities because of
             * context reinitialization, or database changes because of that
             */
            StartupHookManager.registerOrCall(new IStartupHook() {
                @Override
                public void startup() throws Exception {
                    final EntityManager em = context.getEntityManager();
                    try {
                        createIndexNewTx(entityClass, index, em, uniqueNameGenerator);
                    } catch (final Throwable t) {
                        SqlErr.logSqlException(t);
                    }
                }
            });
        }
    }

    @Transactional(propagation = Propagation.NEVER)
    @Override
    public void dropIndexes(final PersistenceUnitContext context, final Class<?> entityClass, final Indexes indexes) {
        final UniqueNameGenerator uniqueNameGenerator = new UniqueNameGenerator();
        for (final Index index : indexes.value()) {
            final EntityManager em = context.getEntityManager();
            try {
                dropIndexNewTx(entityClass, index, em, uniqueNameGenerator);
            } catch (final Throwable t) {
                SqlErr.logSqlException(t);
            }
        }
    }

    @Transactional
    private void createIndexNewTx(final Class<?> entityClass, final Index index, final EntityManager em,
            final UniqueNameGenerator uniqueNameGenerator) {
        Assertions.assertThat(index.columnNames().length).isGreaterThan(0);
        final String comma = ", ";
        final String name;
        if (Strings.isNotBlank(index.name())) {
            name = index.name();
        } else {
            name = "idx" + entityClass.getSimpleName();
        }
        final StringBuilder cols = new StringBuilder();
        final ManagedType<?> managedType = em.getMetamodel().managedType(entityClass);
        for (final String columnName : index.columnNames()) {
            if (cols.length() > 0) {
                cols.append(comma);
            }
            final Attribute<?, ?> column = Attributes.findAttribute(managedType, columnName);
            cols.append(Attributes.extractNativeSqlColumnName(column));
        }

        final String unique;
        if (index.unique()) {
            unique = " UNIQUE";
        } else {
            unique = "";
        }

        final String create = "CREATE" + unique + " INDEX " + uniqueNameGenerator.get(name) + " ON "
                + entityClass.getSimpleName() + " ( " + cols + " )";
        em.createNativeQuery(create).executeUpdate();
    }

    @Transactional
    private void dropIndexNewTx(final Class<?> entityClass, final Index index, final EntityManager em,
            final UniqueNameGenerator uniqueNameGenerator) {
        Assertions.assertThat(index.columnNames().length).isGreaterThan(0);
        final String comma = ", ";
        final String name;
        if (Strings.isNotBlank(index.name())) {
            name = index.name();
        } else {
            name = "idx" + entityClass.getSimpleName();
        }
        final StringBuilder cols = new StringBuilder();
        final ManagedType<?> managedType = em.getMetamodel().managedType(entityClass);
        for (final String columnName : index.columnNames()) {
            if (cols.length() > 0) {
                cols.append(comma);
            }
            final Attribute<?, ?> column = Attributes.findAttribute(managedType, columnName);
            cols.append(Attributes.extractNativeSqlColumnName(column));
        }

        final String create = "DROP INDEX " + uniqueNameGenerator.get(name) + " ON " + entityClass.getSimpleName();
        em.createNativeQuery(create).executeUpdate();
    }

}
