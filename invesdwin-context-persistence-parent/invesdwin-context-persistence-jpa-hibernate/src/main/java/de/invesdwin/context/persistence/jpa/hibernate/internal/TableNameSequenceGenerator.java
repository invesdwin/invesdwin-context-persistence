package de.invesdwin.context.persistence.jpa.hibernate.internal;

import java.util.Properties;

import javax.annotation.concurrent.NotThreadSafe;

import org.hibernate.dialect.Dialect;
import org.hibernate.id.PersistentIdentifierGenerator;
import org.hibernate.id.SequenceGenerator;
import org.hibernate.type.Type;

/**
 * Creates a sequence per table instead of the default behavior of one sequence.
 * 
 * see: http://grails.1312388.n4.nabble.com/One-hibernate-sequence-is-used-for-all-Postgres-tables-td1351722.html
 */
@NotThreadSafe
public class TableNameSequenceGenerator extends SequenceGenerator {

    /**
     * {@inheritDoc} If the parameters do not contain a {@link SequenceGenerator#SEQUENCE} name, we assign one based on
     * the table name.
     */
    @Override
    public void configure(final Type type, final Properties params, final Dialect dialect) {
        if (params.getProperty(SEQUENCE) == null || params.getProperty(SEQUENCE).length() == 0) {
            final String tableName = params.getProperty(PersistentIdentifierGenerator.TABLE);
            if (tableName != null) {
                params.setProperty(SEQUENCE, "seq_" + tableName);
            }
        }
        super.configure(type, params, dialect);
    }
}