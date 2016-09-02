package de.invesdwin.context.persistence.leveldb.ipc.pipe;

import java.io.File;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import com.lts.ipc.IPCPackage;
import com.lts.ipc.fifo.FIFO;

import de.invesdwin.context.persistence.leveldb.ipc.ISynchronousChannel;
import de.invesdwin.context.system.NativeLibrary;
import de.invesdwin.util.lang.Reflections;
import de.invesdwin.util.time.duration.Duration;
import de.invesdwin.util.time.fdate.FTimeUnit;
import ezdb.serde.IntegerSerde;
import ezdb.serde.Serde;

@NotThreadSafe
public abstract class APipeSynchronousChannel implements ISynchronousChannel {

    static {
        new NativeLibrary("clipc", "/clipc/", APipeSynchronousChannel.class);
        IPCPackage.initializeNative();
        Reflections.field("ourPackageInitialized").ofType(boolean.class).in(IPCPackage.class).set(true);
    }

    public static final Serde<Integer> TYPE_SERDE = IntegerSerde.get;
    public static final int TYPE_OFFSET = TYPE_SERDE.toBytes(Integer.MAX_VALUE).length;

    public static final Serde<Integer> SIZE_SERDE = TYPE_SERDE;
    public static final int SIZE_OFFSET = TYPE_OFFSET;

    protected FIFO fifo;
    protected final Duration blockingTimeout;
    private final File file;

    public APipeSynchronousChannel(final File file, final Duration blockingTimeout) {
        this.file = file;
        this.blockingTimeout = blockingTimeout;
    }

    @Override
    public void open() throws IOException {
        this.fifo = new FIFO(file.getAbsolutePath(), blockingTimeout.intValue(FTimeUnit.MILLISECONDS));
    }

    @Override
    public void close() throws IOException {}

}
