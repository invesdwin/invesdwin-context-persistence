package de.invesdwin.context.persistence.mapdb;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiFunction;
import java.util.function.Function;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.mapdb.DB;
import org.mapdb.DB.HashMapMaker;
import org.mapdb.DBMaker;
import org.mapdb.DBMaker.Maker;
import org.mapdb.Serializer;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.lang.reflection.Reflections;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.marshallers.serde.TypeDelegateSerde;

/**
 * If you need to store large data on disk, it is better to use LevelDB only for an ordered index and store the actual
 * db in a file based persistent hash map. This is because LevelDB has very bad insertion speed when handling large
 * elements.
 */
@ThreadSafe
public abstract class ADelegateMapDB<K, V> implements ConcurrentMap<K, V>, Closeable {

    private final String name;
    @GuardedBy("this")
    private ConcurrentMap<K, V> delegate;

    public ADelegateMapDB(final String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    protected synchronized ConcurrentMap<K, V> getDelegate() {
        if (delegate == null) {
            this.delegate = newDelegate();
        }
        return delegate;
    }

    protected ConcurrentMap<K, V> newDelegate() {
        final Maker fileDB = createDB();
        final DB db = configureDB(fileDB).make();
        final HashMapMaker<K, V> maker = db.hashMap(name, newKeySerializier(), newValueSerializer());
        return configureHashMap(maker).createOrOpen();
    }

    protected Maker createDB() {
        final File file = getFile();
        try {
            Files.forceMkdirParent(file);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
        return DBMaker.fileDB(file);
    }

    protected Maker configureDB(final Maker maker) {
        return maker.fileMmapEnable().fileMmapPreclearDisable().cleanerHackEnable().closeOnJvmShutdownWeakReference();
    }

    protected HashMapMaker<K, V> configureHashMap(final HashMapMaker<K, V> maker) {
        return maker.counterEnable();
    }

    private Serializer<K> newKeySerializier() {
        return new SerdeGroupSerializer<K>(newKeySerde());
    }

    private Serializer<V> newValueSerializer() {
        return new SerdeGroupSerializer<V>(newValueSerde());
    }

    protected ISerde<K> newKeySerde() {
        return new TypeDelegateSerde<K>(getKeyType());
    }

    protected ISerde<V> newValueSerde() {
        return new TypeDelegateSerde<V>(getValueType());
    }

    protected File getFile() {
        return new File(getDirectory(), name);
    }

    protected File getDirectory() {
        return new File(getBaseDirectory(), ADelegateMapDB.class.getSimpleName());
    }

    protected File getBaseDirectory() {
        return ContextProperties.getHomeDataDirectory();
    }

    @SuppressWarnings("unchecked")
    private Class<K> getKeyType() {
        return (Class<K>) Reflections.resolveTypeArguments(getClass(), ADelegateMapDB.class)[0];
    }

    @SuppressWarnings("unchecked")
    private Class<V> getValueType() {
        return (Class<V>) Reflections.resolveTypeArguments(getClass(), ADelegateMapDB.class)[1];
    }

    @Override
    public int size() {
        return getDelegate().size();
    }

    @Override
    public boolean isEmpty() {
        return getDelegate().isEmpty();
    }

    @Override
    public boolean containsKey(final Object key) {
        return getDelegate().containsKey(key);
    }

    @Override
    public boolean containsValue(final Object value) {
        return getDelegate().containsValue(value);
    }

    @Override
    public V get(final Object key) {
        return getDelegate().get(key);
    }

    @Override
    public V put(final K key, final V value) {
        return getDelegate().put(key, value);
    }

    @Override
    public V computeIfAbsent(final K key, final Function<? super K, ? extends V> mappingFunction) {
        return getDelegate().computeIfAbsent(key, mappingFunction);
    }

    @Override
    public V compute(final K key, final BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
        return getDelegate().compute(key, remappingFunction);
    }

    @Override
    public V computeIfPresent(final K key, final BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
        return getDelegate().computeIfPresent(key, remappingFunction);
    }

    @Override
    public V remove(final Object key) {
        return getDelegate().remove(key);
    }

    @Override
    public void putAll(final Map<? extends K, ? extends V> m) {
        getDelegate().putAll(m);
    }

    @Override
    public void clear() {
        getDelegate().clear();
    }

    @Override
    public Set<K> keySet() {
        return getDelegate().keySet();
    }

    @Override
    public Collection<V> values() {
        return getDelegate().values();
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return getDelegate().entrySet();
    }

    @Override
    public V putIfAbsent(final K key, final V value) {
        return getDelegate().putIfAbsent(key, value);
    }

    @Override
    public boolean remove(final Object key, final Object value) {
        return getDelegate().remove(key, value);
    }

    @Override
    public boolean replace(final K key, final V oldValue, final V newValue) {
        return getDelegate().replace(key, oldValue, newValue);
    }

    @Override
    public V replace(final K key, final V value) {
        return getDelegate().replace(key, value);
    }

    @Override
    public synchronized void close() {
        if (delegate != null) {
            final Closeable cDelegate = (Closeable) delegate;
            try {
                cDelegate.close();
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
            delegate = null;
        }
    }

    public synchronized void deleteTable() {
        close();
        Files.deleteQuietly(getFile());
    }

}
