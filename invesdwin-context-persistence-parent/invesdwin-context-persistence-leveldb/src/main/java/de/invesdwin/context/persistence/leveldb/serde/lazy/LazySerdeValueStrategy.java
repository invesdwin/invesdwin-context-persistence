package de.invesdwin.context.persistence.leveldb.serde.lazy;

import javax.annotation.concurrent.NotThreadSafe;

import ezdb.serde.Serde;

@NotThreadSafe
public enum LazySerdeValueStrategy {
    REMOTE {
        @Override
        public <T> ILazySerdeValueSerde<? extends T> getValueSerdeOverride(final Serde<T> valueSerde) {
            return new RemoteLazySerdeValueSerde<T>(valueSerde, true);
        }
    },
    LOCAL {
        @Override
        public <T> ILazySerdeValueSerde<? extends T> getValueSerdeOverride(final Serde<T> valueSerde) {
            return new LocalLazySerdeValueSerde<T>(valueSerde);
        }
    };

    public static LazySerdeValueStrategy fromBoolean(final boolean remote) {
        if (remote) {
            return REMOTE;
        } else {
            return LOCAL;
        }
    }

    public abstract <T> ILazySerdeValueSerde<? extends T> getValueSerdeOverride(Serde<T> valueSerde);

}
