package de.invesdwin.context.persistence.leveldb.mapdb;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.mapdb.DB;
import org.mapdb.DB.HashMapMaker;
import org.mapdb.DBMaker;
import org.mapdb.DBMaker.Maker;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.Serializer;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.streams.LZ4Streams;
import de.invesdwin.context.persistence.leveldb.serde.ExtendedTypeDelegateSerde;
import de.invesdwin.util.lang.Reflections;
import ezdb.serde.Serde;

/**
 * If you need to store large data on disk, it is better to use LevelDB only for an ordered index and store the actual
 * db in a file based persistent hash map. This is because LevelDB has very bad insertion speed when handling large
 * elements.
 */
@ThreadSafe
public abstract class ADelegateMapDB<K extends Serializable, V extends Serializable>
        implements ConcurrentMap<K, V>, Closeable {

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
        return DBMaker.fileDB(getFile());
    }

    protected Maker configureDB(final Maker maker) {
        //        return maker.fileMmapEnable().fileMmapPreclearDisable().cleanerHackEnable();
        //file channel supports parallel writes
        return maker.fileChannelEnable();
    }

    protected HashMapMaker<K, V> configureHashMap(final HashMapMaker<K, V> maker) {
        return maker.counterEnable();
    }

    private Serializer<V> newValueSerializer() {
        return newSerializer(newValueSerde());
    }

    private Serializer<K> newKeySerializier() {
        return newSerializer(newKeySerde());
    }

    private <T> Serializer<T> newSerializer(final Serde<T> serde) {
        return new Serializer<T>() {

            @Override
            public void serialize(final DataOutput2 out, final T value) throws IOException {
                final byte[] entry;
                entry = serde.toBytes(value);
                final OutputStream compressor = newCompressor(out);
                try {
                    IOUtils.write(entry, compressor);
                } catch (final IOException e) {
                    throw new RuntimeException(e);
                } finally {
                    IOUtils.closeQuietly(compressor);
                }
            }

            @Override
            public T deserialize(final DataInput2 input, final int available) throws IOException {
                final InputStream bis = new DataInput2.DataInputToStream(input);
                final InputStream decompressor = newDecompressor(bis);
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
                    IOUtils.closeQuietly(decompressor);
                }
            }

            @Override
            public boolean isTrusted() {
                return true;
            }

        };
    }

    protected InputStream newDecompressor(final InputStream in) {
        return LZ4Streams.newDefaultLZ4BlockInputStream(in);
    }

    protected OutputStream newCompressor(final OutputStream out) {
        return LZ4Streams.newDefaultLZ4BlockOutputStream(out);
    }

    protected Serde<K> newKeySerde() {
        return new ExtendedTypeDelegateSerde<K>(getKeyType());
    }

    protected Serde<V> newValueSerde() {
        return new ExtendedTypeDelegateSerde<V>(getValueType());
    }

    protected File getFile() {
        return new File(getDirectory(), name);
    }

    protected File getDirectory() {
        return new File(getBaseDirectory(), ADelegateMapDB.class.getSimpleName());
    }

    protected File getBaseDirectory() {
        return ContextProperties.getHomeDirectory();
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
        FileUtils.deleteQuietly(getFile());
    }

}
