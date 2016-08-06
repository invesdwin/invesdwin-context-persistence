package de.invesdwin.context.persistence.jpa.kundera;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.persistence.jpa.PersistenceUnitContext;
import de.invesdwin.context.persistence.jpa.api.index.Index;
import de.invesdwin.context.persistence.jpa.api.index.Indexes;
import de.invesdwin.context.persistence.jpa.api.util.Attributes;
import de.invesdwin.context.persistence.jpa.spi.IIndexCreationHandler;
import de.invesdwin.util.lang.Strings;

@NotThreadSafe
public class KunderaIndexCreationHandler implements IIndexCreationHandler {

    @Override
    public void createIndexes(final PersistenceUnitContext context, final Class<?> entityClass, final Indexes indexes) {
        final Set<String> columnNames = new LinkedHashSet<String>();
        for (final Index index : indexes.value()) {
            for (final String columnName : index.columnNames()) {
                columnNames.add(Strings.removeEnd(columnName, Attributes.ID_SUFFIX));
            }
        }
        final List<com.impetus.kundera.index.Index> kunderaIndexes = new ArrayList<com.impetus.kundera.index.Index>();
        for (final String columnName : columnNames) {
            kunderaIndexes.add(new com.impetus.kundera.index.Index() {
                @Override
                public Class<? extends Annotation> annotationType() {
                    return com.impetus.kundera.index.Index.class;
                }

                @Override
                public String name() {
                    return columnName;
                }

                @Override
                public String type() {
                    return "";
                }

                @Override
                public int max() {
                    return Integer.MAX_VALUE;
                }

                @Override
                public int min() {
                    return Integer.MIN_VALUE;
                }

                @Override
                public String indexName() {
                    //TODO
                    return null;
                }
            });
        }
        final com.impetus.kundera.index.Index[] kunderaIndexesArray = kunderaIndexes
                .toArray(new com.impetus.kundera.index.Index[0]);
        //        Assertions.assertThat(Reflections.addAnnotation(entityClass, new com.impetus.kundera.index.IndexCollection() {
        //            @Override
        //            public Class<? extends Annotation> annotationType() {
        //                return com.impetus.kundera.index.IndexCollection.class;
        //            }
        //
        //            @Override
        //            public com.impetus.kundera.index.Index[] columns() {
        //                return kunderaIndexesArray;
        //            }
        //        })).isNull();
        //TODO: add the annotation using javassist/instrumentationHook http://ayoubelabbassi.blogspot.de/2011/01/how-to-add-annotations-at-runtime-to.html
        //for now one has to use the original kundera annotations...
    }

    @Override
    public void dropIndexes(final PersistenceUnitContext context, final Class<?> entityClass, final Indexes indexes) {
        //        throw new UnsupportedOperationException();
    }

}
