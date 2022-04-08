package de.invesdwin.context.persistence.leveldb.serde;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.lang.reflection.Reflections;
import de.invesdwin.util.time.date.FDate;
import ezdb.serde.Serde;
import ezdb.serde.SerializingSerde;
import ezdb.serde.TypeDelegateSerde;

@Immutable
public class ExtendedTypeDelegateSerde<O> extends TypeDelegateSerde<O> {

    public ExtendedTypeDelegateSerde(final Class<O> type) {
        super(type);
    }

    @Override
    protected Serde<?> newDelegate(final Class<O> type) {
        if (Reflections.isVoid(type)) {
            return VoidSerde.get;
        }
        if (FDate.class.isAssignableFrom(type)) {
            return FDateSerde.get;
        }
        final Serde<?> serde = super.newDelegate(type);
        if (serde instanceof SerializingSerde) {
            return new FastSerializingSerde<O>(true, type);
        } else {
            return serde;
        }
    }

}
