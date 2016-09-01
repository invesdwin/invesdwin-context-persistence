package de.invesdwin.context.persistence.leveldb.serde.lazy.factory;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.persistence.leveldb.serde.lazy.ILazySerdeValue;
import ezdb.serde.Serde;

@NotThreadSafe
public final class ThreadLocalLazySerdeValueFactory implements ILazySerdeValueFactory {

    public static final ThreadLocalLazySerdeValueFactory INSTANCE = new ThreadLocalLazySerdeValueFactory();

    private final ThreadLocal<ILazySerdeValueFactory> strategy = new ThreadLocal<ILazySerdeValueFactory>() {
        @Override
        protected ILazySerdeValueFactory initialValue() {
            return LazySerdeValueFactoryStrategy.EAGER_VALUE_ONLY.getStrategy();
        }
    };

    private ThreadLocalLazySerdeValueFactory() {}

    @Override
    public <T> ILazySerdeValue<T> fromValue(final Serde<T> serde, final T value) {
        return strategy.get().fromValue(serde, value);
    }

    @Override
    public <T> ILazySerdeValue<T> fromBytes(final Serde<T> serde, final byte[] bytes) {
        return strategy.get().fromBytes(serde, bytes);
    }

    public ThreadLocal<ILazySerdeValueFactory> getStrategy() {
        return strategy;
    }

}
