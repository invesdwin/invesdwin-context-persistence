package de.invesdwin.context.persistence.timeseriesdb;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.persistentmap.IPersistentMapFactory;
import de.invesdwin.context.integration.persistentmap.PersistentHeapMapFactory;
import de.invesdwin.context.integration.persistentmap.large.LargePersistentMapFactory;
import de.invesdwin.context.persistence.chronicle.PersistentChronicleMapFactory;
import de.invesdwin.context.persistence.ezdb.PersistentEzdbMapFactory;
import de.invesdwin.context.persistence.mapdb.PersistentMapDBFactory;

@Immutable
public enum PersistentMapType implements IPersistentMapType {
    ON_HEAP {
        @Override
        public <K, V> IPersistentMapFactory<K, V> newFactory() {
            //not actually persistent
            return new PersistentHeapMapFactory<>();
        }

        @Override
        public boolean isRemoveFullySupported() {
            return true;
        }
    },
    OFF_HEAP {
        @Override
        public <K, V> IPersistentMapFactory<K, V> newFactory() {
            //otherwise use chronicle map for faster put/get
            return new PersistentChronicleMapFactory<K, V>() {
                @Override
                public boolean isDiskPersistenceSupported() {
                    return false;
                }
            };
        }

        @Override
        public boolean isRemoveFullySupported() {
            //chronicle map does not really support deleting entries, the file size gets bloaded which causes significant I/O
            return false;
        }
    },
    DISK_FAST {
        @Override
        public <K, V> IPersistentMapFactory<K, V> newFactory() {
            //otherwise use chronicle map for faster put/get
            return new PersistentChronicleMapFactory<>();
        }

        @Override
        public boolean isRemoveFullySupported() {
            //chronicle map does not really support deleting entries, the file size gets bloaded which causes significant I/O
            return false;
        }
    },
    DISK_SAFE {
        @Override
        public <K, V> IPersistentMapFactory<K, V> newFactory() {
            //leveldb does not have any hiccups regarding force closed applications
            return new PersistentEzdbMapFactory<>();
        }

        @Override
        public boolean isRemoveFullySupported() {
            return true;
        }
    },
    DISK_MEDIUM {
        @Override
        public <K, V> IPersistentMapFactory<K, V> newFactory() {
            //mapdb has a smaller footprint on disk, thus prefer that for larger entries
            return new PersistentMapDBFactory<K, V>();
        }

        @Override
        public boolean isRemoveFullySupported() {
            return true;
        }
    },
    DISK_LARGE_FAST {
        @Override
        public <K, V> IPersistentMapFactory<K, V> newFactory() {
            return new LargePersistentMapFactory<>(DISK_FAST.newFactory());
        }

        @Override
        public boolean isRemoveFullySupported() {
            //the mapped memory value storage does not support removals, items are only removed from the index
            return false;
        }
    },
    DISK_LARGE_SAFE {
        @Override
        public <K, V> IPersistentMapFactory<K, V> newFactory() {
            return new LargePersistentMapFactory<>(DISK_SAFE.newFactory());
        }

        @Override
        public boolean isRemoveFullySupported() {
            //the mapped memory value storage does not support removals, items are only removed from the index
            return false;
        }
    },
    DISK_LARGE_MEDIUM {
        @Override
        public <K, V> IPersistentMapFactory<K, V> newFactory() {
            return new LargePersistentMapFactory<>(DISK_MEDIUM.newFactory());
        }

        @Override
        public boolean isRemoveFullySupported() {
            //the mapped memory value storage does not support removals, items are only removed from the index
            return false;
        }
    };

    @Override
    public abstract <K, V> IPersistentMapFactory<K, V> newFactory();

}
