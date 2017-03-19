package de.invesdwin.context.persistence.jpa.scanning.transaction;

import java.util.Stack;

import javax.annotation.concurrent.ThreadSafe;

import org.slf4j.MDC;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;

import de.invesdwin.context.log.Log;
import de.invesdwin.context.persistence.jpa.PersistenceUnitContext;
import de.invesdwin.context.persistence.jpa.spi.impl.PersistenceUnitAnnotationUtil;
import de.invesdwin.util.lang.Strings;
import io.netty.util.concurrent.FastThreadLocal;

@ThreadSafe
public class LoggingDelegateTransactionManager extends ADelegateTransactionManager {

    private static final String MDC_KEY = "transactions";
    private static final FastThreadLocal<Stack<String>> TRANSACTIONS_NDC = new FastThreadLocal<Stack<String>>() {
        @Override
        protected Stack<String> initialValue() {
            return new Stack<String>();
        };
    };
    private static final Log LOG = new Log("de.invesdwin.TRANSACTIONS");
    private final PlatformTransactionManager delegate;
    private final PersistenceUnitContext context;

    public LoggingDelegateTransactionManager(final PersistenceUnitContext context,
            final PlatformTransactionManager delegate) {
        this.context = context;
        this.delegate = delegate;
    }

    /**
     * This constructor fixes reinitialization problems during tests
     */
    public LoggingDelegateTransactionManager() {
        this(null, new PlatformTransactionManager() {

            @Override
            public void rollback(final TransactionStatus status) {}

            @Override
            public TransactionStatus getTransaction(final TransactionDefinition definition) {
                return null;
            }

            @Override
            public void commit(final TransactionStatus status) {}
        });
    }

    @Override
    public TransactionStatus getTransaction(final TransactionDefinition definition) {
        final TransactionStatus status = super.getTransaction(definition);
        if (status != null && status.isNewTransaction()) {
            final String info = begin(status);
            if (info != null) {
                LOG.info(info);
            }
        }
        return status;
    }

    @Override
    public void commit(final TransactionStatus status) {
        super.commit(status);
        if (status != null && status.isNewTransaction()) {
            final String info = close("commit");
            if (info != null) {
                LOG.info(info);
            }
        }
    }

    @Override
    public void rollback(final TransactionStatus status) {
        super.rollback(status);
        if (status != null && status.isNewTransaction()) {
            final String info = close("rollback");
            if (info != null) {
                LOG.info(info);
            }
        }
    }

    private String begin(final TransactionStatus status) {
        final Stack<String> ndc = TRANSACTIONS_NDC.get();
        final String id = String.valueOf(status.hashCode());
        ndc.push(id);
        StringBuilder info = null;
        if (LOG.isInfoEnabled()) {
            info = new StringBuilder("begin ");
            info.append(PersistenceUnitAnnotationUtil.PERSISTENCE_UNIT_CONFIG_PREFIX);
            info.append(context.getPersistenceUnitName());
            info.append(" #");
            info.append(ndc.size());
            info.append(": ");
            info.append(ndc);
        }
        MDC.put(MDC_KEY, String.valueOf(ndc.size()));
        return Strings.asString(info);
    }

    /**
     * To make the methodname occur in the log, the calling method needs to log the message.
     */
    private String close(final String action) {
        StringBuilder info = null;
        final Stack<String> ndc = TRANSACTIONS_NDC.get();
        if (LOG.isInfoEnabled()) {
            info = new StringBuilder(action);
            info.append(" ");
            info.append(PersistenceUnitAnnotationUtil.PERSISTENCE_UNIT_CONFIG_PREFIX);
            info.append(context.getPersistenceUnitName());
            info.append(" #");
            info.append(ndc.size());
            info.append(": ");
            info.append(ndc);
        }
        ndc.pop();
        if (ndc.size() > 0) {
            MDC.put(MDC_KEY, String.valueOf(ndc.size()));
        } else {
            TRANSACTIONS_NDC.remove();
            MDC.remove(MDC_KEY);
        }
        return Strings.asString(info);
    }

    @Override
    protected PlatformTransactionManager createDelegate() {
        return delegate;
    }

}
