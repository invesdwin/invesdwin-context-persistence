package de.invesdwin.context.persistence.timeseries.mapdb;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.function.BiFunction;
import java.util.function.Function;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DB.TreeMapMaker;
import org.mapdb.DBMaker;
import org.mapdb.DBMaker.Maker;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.serializer.GroupSerializer;
import org.mapdb.serializer.GroupSerializerObjectArray;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.streams.compressor.lz4.LZ4Streams;
import de.invesdwin.util.lang.Closeables;
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
public abstract class ADelegateTreeMapDB<K, V> implements ConcurrentNavigableMap<K, V>, Closeable {

    private final String name;
    @GuardedBy("this")
    private BTreeMap<K, V> delegate;

    public ADelegateTreeMapDB(final String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    protected synchronized BTreeMap<K, V> getDelegate() {
        if (delegate == null) {
            this.delegate = newDelegate();
        }
        return delegate;
    }

    protected BTreeMap<K, V> newDelegate() {
        final Maker fileDB = createDB();
        final DB db = configureDB(fileDB).make();
        final TreeMapMaker<K, V> maker = db.treeMap(name, newKeySerializier(), newValueSerializer());
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
        //        return maker.fileMmapEnable().fileMmapPreclearDisable().cleanerHackEnable();
        //file channel supports parallel writes
        return maker.fileChannelEnable();
    }

    protected TreeMapMaker<K, V> configureHashMap(final TreeMapMaker<K, V> maker) {
        return maker.counterEnable();
    }

    private GroupSerializer<V> newValueSerializer() {
        return newSerializer(newValueSerde());
    }

    private GroupSerializer<K> newKeySerializier() {
        return newSerializer(newKeySerde());
    }

    private <T> GroupSerializer<T> newSerializer(final ISerde<T> serde) {
        return new GroupSerializerObjectArray<T>() {

            @Override
            public void serialize(final DataOutput2 out, final T value) throws IOException {
                final byte[] entry;
                entry = serde.toBytes(value);
                final OutputStream compressor = newCompressor(out);
                if (compressor == out) {
                    BYTE_ARRAY.serialize(out, entry);
                } else {
                    try {
                        IOUtils.write(entry, compressor);
                    } catch (final IOException e) {
                        throw new RuntimeException(e);
                    } finally {
                        Closeables.closeQuietly(compressor);
                    }
                }
            }

            @Override
            public T deserialize(final DataInput2 input, final int available) throws IOException {
                final InputStream bis = new DataInput2.DataInputToStream(input);
                final InputStream decompressor = newDecompressor(bis);
                if (decompressor == bis) {
                    final byte[] bytes = BYTE_ARRAY.deserialize(input, -1);
                    return serde.fromBytes(bytes);
                } else {
                    try {
                        final ByteArrayOutputStream bos = new ByteArrayOutputStream();
                        try {
                            IOUtils.copy(decompressor, bos);
                        } catch (final IOException e) {
                            //ignore, end reached
                        }
                        final byte[] bytes = bos.toByteArray();
                        return serde.fromBytes(bytes);
                    } finally {
                        Closeables.closeQuietly(decompressor);
                    }
                }
            }

            @Override
            public boolean isTrusted() {
                return true;
            }

        };
    }

    protected InputStream newDecompressor(final InputStream in) {
        return LZ4Streams.newDefaultLZ4InputStream(in);
    }

    protected OutputStream newCompressor(final OutputStream out) {
        return LZ4Streams.newDefaultLZ4OutputStream(out);
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
        return new File(getBaseDirectory(), ADelegateTreeMapDB.class.getSimpleName());
    }

    protected File getBaseDirectory() {
        return ContextProperties.getHomeDataDirectory();
    }

    @SuppressWarnings("unchecked")
    private Class<K> getKeyType() {
        return (Class<K>) Reflections.resolveTypeArguments(getClass(), ADelegateTreeMapDB.class)[0];
    }

    @SuppressWarnings("unchecked")
    private Class<V> getValueType() {
        return (Class<V>) Reflections.resolveTypeArguments(getClass(), ADelegateTreeMapDB.class)[1];
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
    public NavigableSet<K> keySet() {
        return getDelegate().navigableKeySet();
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
            final Closeable cDelegate = delegate;
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

    @Override
    public Comparator<? super K> comparator() {
        return getDelegate().comparator();
    }

    @Override
    public K firstKey() {
        return getDelegate().firstKey();
    }

    @Override
    public K lastKey() {
        return getDelegate().lastKey();
    }

    @Override
    public Entry<K, V> lowerEntry(final K key) {
        return getDelegate().lowerEntry(key);
    }

    @Override
    public K lowerKey(final K key) {
        return getDelegate().lowerKey(key);
    }

    @Override
    public Entry<K, V> floorEntry(final K key) {
        return getDelegate().floorEntry(key);
    }

    @Override
    public K floorKey(final K key) {
        return getDelegate().floorKey(key);
    }

    @Override
    public Entry<K, V> ceilingEntry(final K key) {
        return getDelegate().ceilingEntry(key);
    }

    @Override
    public K ceilingKey(final K key) {
        return getDelegate().ceilingKey(key);
    }

    @Override
    public Entry<K, V> higherEntry(final K key) {
        return getDelegate().higherEntry(key);
    }

    @Override
    public K higherKey(final K key) {
        return getDelegate().higherKey(key);
    }

    @Override
    public Entry<K, V> firstEntry() {
        return getDelegate().firstEntry();
    }

    @Override
    public Entry<K, V> lastEntry() {
        return getDelegate().lastEntry();
    }

    @Override
    public Entry<K, V> pollFirstEntry() {
        return getDelegate().pollFirstEntry();
    }

    @Override
    public Entry<K, V> pollLastEntry() {
        return getDelegate().pollLastEntry();
    }

    @Override
    public ConcurrentNavigableMap<K, V> descendingMap() {
        return getDelegate().descendingMap();
    }

    @Override
    public NavigableSet<K> navigableKeySet() {
        return getDelegate().navigableKeySet();
    }

    @Override
    public NavigableSet<K> descendingKeySet() {
        return getDelegate().descendingKeySet();
    }

    @Override
    public ConcurrentNavigableMap<K, V> subMap(final K fromKey, final boolean fromInclusive, final K toKey,
            final boolean toInclusive) {
        return getDelegate().subMap(fromKey, fromInclusive, toKey, toInclusive);
    }

    @Override
    public ConcurrentNavigableMap<K, V> headMap(final K toKey, final boolean inclusive) {
        return getDelegate().headMap(toKey, inclusive);
    }

    @Override
    public ConcurrentNavigableMap<K, V> tailMap(final K fromKey, final boolean inclusive) {
        return getDelegate().tailMap(fromKey, inclusive);
    }

    @Override
    public ConcurrentNavigableMap<K, V> subMap(final K fromKey, final K toKey) {
        return getDelegate().subMap(fromKey, toKey);
    }

    @Override
    public ConcurrentNavigableMap<K, V> headMap(final K toKey) {
        return getDelegate().headMap(toKey);
    }

    @Override
    public ConcurrentNavigableMap<K, V> tailMap(final K fromKey) {
        return getDelegate().tailMap(fromKey);
    }

}
