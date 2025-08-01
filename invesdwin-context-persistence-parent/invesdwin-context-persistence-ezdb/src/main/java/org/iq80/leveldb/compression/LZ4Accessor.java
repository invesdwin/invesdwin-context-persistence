package org.iq80.leveldb.compression;

import javax.annotation.concurrent.Immutable;

@Immutable
public final class LZ4Accessor {

    public static final Compression LZ4FAST = LZ4.LZ4FAST;
    public static final Compression LZ4HC = LZ4.LZ4HC;

    private LZ4Accessor() {}

}
