package de.invesdwin.context.persistence.leveldb.serde;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.persistence.leveldb.serde.lazy.LazySerdeValue;
import de.invesdwin.context.persistence.leveldb.serde.lazy.LazySerdeValueSerde;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.lang.Reflections;
import de.invesdwin.util.time.fdate.FDate;
import ezdb.serde.Serde;
import ezdb.serde.SerializingSerde;
import ezdb.serde.TypeDelegateSerde;

@Immutable
public class ExtendedTypeDelegateSerde<O> extends TypeDelegateSerde<O> {

    public ExtendedTypeDelegateSerde(final Class<O> type) {
        super(type);
        Assertions.checkNotEquals(type, LazySerdeValue.class, "Please provide your own instance of %s",
                LazySerdeValueSerde.class.getSimpleName());
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
