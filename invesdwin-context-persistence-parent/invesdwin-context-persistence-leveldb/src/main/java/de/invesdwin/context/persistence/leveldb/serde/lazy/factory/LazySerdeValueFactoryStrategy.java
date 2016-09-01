package de.invesdwin.context.persistence.leveldb.serde.lazy.factory;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.persistence.leveldb.serde.lazy.ILazySerdeValue;
import de.invesdwin.context.persistence.leveldb.serde.lazy.LazySerdeValue;
import ezdb.serde.Serde;

@Immutable
public enum LazySerdeValueFactoryStrategy {
    LAZY(new ILazySerdeValueFactory() {

        @Override
        public <T> ILazySerdeValue<T> fromValue(final Serde<T> serde, final T value) {
            return new LazySerdeValue<T>(serde, value);
        }

        @Override
        public <T> ILazySerdeValue<T> fromBytes(final Serde<T> serde, final byte[] bytes) {
            return new LazySerdeValue<T>(serde, bytes);
        }
    }),
    EAGER(new ILazySerdeValueFactory() {

        @Override
        public <T> ILazySerdeValue<T> fromValue(final Serde<T> serde, final T value) {
            final byte[] bytes = serde.toBytes(value);
            return new ILazySerdeValue<T>() {
                @Override
                public byte[] getBytes() {
                    return bytes;
                }

                @Override
                public T getValue() {
                    return value;
                }
            };
        }

        @Override
        public <T> ILazySerdeValue<T> fromBytes(final Serde<T> serde, final byte[] bytes) {
            final T value = serde.fromBytes(bytes);
            return new ILazySerdeValue<T>() {
                @Override
                public byte[] getBytes() {
                    return bytes;
                }

                @Override
                public T getValue() {
                    return value;
                }
            };
        }
    }),
    EAGER_VALUE_ONLY(new ILazySerdeValueFactory() {

        @Override
        public <T> ILazySerdeValue<T> fromValue(final Serde<T> serde, final T value) {
            return new ILazySerdeValue<T>() {
                @Override
                public byte[] getBytes() {
                    throw new UnsupportedOperationException();
                }

                @Override
                public T getValue() {
                    return value;
                }
            };
        }

        @Override
        public <T> ILazySerdeValue<T> fromBytes(final Serde<T> serde, final byte[] bytes) {
            final T value = serde.fromBytes(bytes);
            return new ILazySerdeValue<T>() {
                @Override
                public byte[] getBytes() {
                    throw new UnsupportedOperationException();
                }

                @Override
                public T getValue() {
                    return value;
                }
            };
        }
    });

    private ILazySerdeValueFactory strategy;

    LazySerdeValueFactoryStrategy(final ILazySerdeValueFactory strategy) {
        this.strategy = strategy;
    }

    public ILazySerdeValueFactory getStrategy() {
        return strategy;
    }

}
