package de.invesdwin.context.persistence.timeseriesdb;

import javax.annotation.concurrent.Immutable;

import org.mapdb.DBMaker.Maker;

import de.invesdwin.context.integration.persistentmap.APersistentMapConfig;
import de.invesdwin.context.integration.persistentmap.IPersistentMapFactory;
import de.invesdwin.context.persistence.chronicle.PersistentChronicleMapFactory;
import de.invesdwin.context.persistence.ezdb.PersistentEzdbMapFactory;
import de.invesdwin.context.persistence.mapdb.PersistentMapDBFactory;

@Immutable
public enum PersistentMapType {
    FAST {
        @Override
        public <K, V> IPersistentMapFactory<K, V> newFactory() {
            //otherwise use chronicle map for faster put/get
            return new PersistentChronicleMapFactory<>();
        }
    },
    SAFE {
        @Override
        public <K, V> IPersistentMapFactory<K, V> newFactory() {
            //leveldb does not have any hiccups regarding force closed applications
            return new PersistentEzdbMapFactory<>();
        }
    },
    LARGE {
        @Override
        public <K, V> IPersistentMapFactory<K, V> newFactory() {
            //mapdb has a smaller footprint on disk, thus prefer that for larger entries
            return new PersistentMapDBFactory<K, V>();
        }
    },
    LARGE_SAFE {
        @Override
        public <K, V> IPersistentMapFactory<K, V> newFactory() {
            //mapdb might corrupt data (with lz4) in highly parallel scenarios when mmap is used, for those cases it is better to use the slower file channels
            return new PersistentMapDBFactory<K, V>() {
                @Override
                protected Maker configureDB(final APersistentMapConfig<K, V> config, final Maker maker) {
                    return maker.fileChannelEnable()
                            .closeOnJvmShutdownWeakReference()
                            .cleanerHackEnable()
                            .checksumHeaderBypass();
                }
            };
        }
    };

    public abstract <K, V> IPersistentMapFactory<K, V> newFactory();

}
