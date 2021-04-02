package de.invesdwin.context.persistence.jpa.scanning.transaction;

import java.util.Deque;
import java.util.LinkedList;

import javax.annotation.concurrent.ThreadSafe;

import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;

import de.invesdwin.context.persistence.jpa.PersistenceProperties;
import de.invesdwin.context.persistence.jpa.PersistenceUnitContext;
import de.invesdwin.util.collections.loadingcache.ALoadingCache;
import de.invesdwin.util.lang.Strings;
import de.invesdwin.util.lang.reflection.Reflections;
import io.netty.util.concurrent.FastThreadLocal;

@ThreadSafe
public class ContextDelegatingTransactionManager implements PlatformTransactionManager {

    private static final TransactionStatus NO_TRANSACTION = new DisabledTransactionStatus();
    private static final FastThreadLocal<Deque<PersistenceUnitContext>> CUR_CONTEXT = new FastThreadLocal<Deque<PersistenceUnitContext>>() {
        @Override
        protected Deque<PersistenceUnitContext> initialValue() {
            return new LinkedList<PersistenceUnitContext>();
        };
    };
    private static boolean enabled;
    private final ALoadingCache<String, PersistenceUnitContext> className_persistenceUnitContext = new ALoadingCache<String, PersistenceUnitContext>() {
        @Override
        protected PersistenceUnitContext loadValue(final String key) {
            final Class<?> clazz = Reflections.classForName(key);
            return PersistenceProperties.getPersistenceUnitContext(clazz);
        }
    };

    @Override
    public TransactionStatus getTransaction(final TransactionDefinition definition) {
        if (!enabled) {
            return null;
        }
        final Deque<PersistenceUnitContext> deque = CUR_CONTEXT.get();
        if (definition.getPropagationBehavior() == TransactionDefinition.PROPAGATION_NEVER && deque.size() == 0) {
            /*
             * don't delegate if no transaction is desired and currently none exists; this resolves some bugs with
             * updates not being recognized properly in queries
             */
            return NO_TRANSACTION;
        } else {
            //since it is <classname>.<methodname> we can use string operations here
            final PersistenceUnitContext context;
            if (definition.getName() != null) {
                final String className = Strings.substringBeforeLast(definition.getName(), ".");
                context = className_persistenceUnitContext.get(className);
            } else {
                context = PersistenceProperties
                        .getPersistenceUnitContext(PersistenceProperties.DEFAULT_PERSISTENCE_UNIT_NAME);
            }
            deque.addLast(context);
            return context.getTransactionManager().getTransaction(definition);
        }
    }

    @Override
    public void commit(final TransactionStatus status) {
        if (status == NO_TRANSACTION) {
            return;
        }
        final Deque<PersistenceUnitContext> deque = CUR_CONTEXT.get();
        final PersistenceUnitContext context = deque.removeLast();
        if (deque.isEmpty()) {
            CUR_CONTEXT.remove();
        }
        context.getTransactionManager().commit(status);
    }

    @Override
    public void rollback(final TransactionStatus status) {
        if (status == NO_TRANSACTION) {
            return;
        }
        final Deque<PersistenceUnitContext> deque = CUR_CONTEXT.get();
        final PersistenceUnitContext context = deque.removeLast();
        if (deque.isEmpty()) {
            CUR_CONTEXT.remove();
        }
        context.getTransactionManager().rollback(status);
    }

    public static void setEnabled(final boolean enabled) {
        ContextDelegatingTransactionManager.enabled = enabled;
    }

}
