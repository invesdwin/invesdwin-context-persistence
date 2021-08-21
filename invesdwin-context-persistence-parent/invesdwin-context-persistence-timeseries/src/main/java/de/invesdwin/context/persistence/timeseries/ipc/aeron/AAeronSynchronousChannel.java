package de.invesdwin.context.persistence.timeseries.ipc.aeron;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.persistence.timeseries.ipc.ISynchronousChannel;
import io.aeron.Aeron;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.MediaDriver.Context;
import io.aeron.driver.ThreadingMode;

@NotThreadSafe
public abstract class AAeronSynchronousChannel implements ISynchronousChannel {

    public static final MediaDriver MEDIA_DRIVER = MediaDriver.launchEmbedded(
            new Context().dirDeleteOnShutdown(true).dirDeleteOnStart(true).threadingMode(ThreadingMode.SHARED));

    protected static final int TYPE_INDEX = 0;
    protected static final int TYPE_SIZE = Integer.SIZE;

    protected static final int SEQUENCE_INDEX = TYPE_INDEX + TYPE_SIZE;
    protected static final int SEQUENCE_SIZE = Integer.SIZE;

    protected static final int MESSAGE_INDEX = SEQUENCE_INDEX + SEQUENCE_SIZE;

    protected final String channel;
    protected final int streamId;
    protected Aeron aeron;

    public AAeronSynchronousChannel(final String channel, final int streamId) {
        this.channel = channel;
        this.streamId = streamId;
    }

    @Override
    public void open() throws IOException {
        this.aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(MEDIA_DRIVER.aeronDirectoryName()));
    }

    @Override
    public void close() throws IOException {
        if (aeron != null) {
            aeron.close();
            aeron = null;
        }
    }

}
