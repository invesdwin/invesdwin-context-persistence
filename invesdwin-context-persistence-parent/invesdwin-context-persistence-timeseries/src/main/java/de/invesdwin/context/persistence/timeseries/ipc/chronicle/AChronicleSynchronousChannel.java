package de.invesdwin.context.persistence.timeseries.ipc.chronicle;

import java.io.File;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.persistence.timeseries.ipc.ISynchronousChannel;
import de.invesdwin.util.lang.Files;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;

@NotThreadSafe
public abstract class AChronicleSynchronousChannel implements ISynchronousChannel {

    protected ChronicleQueue queue;
    protected final File file;

    public AChronicleSynchronousChannel(final File file) {
        this.file = file;
    }

    @Override
    public void open() throws IOException {
        try {
            this.queue = SingleChronicleQueueBuilder.binary(file)
                    .rollCycle(RollCycles.MINUTELY)
                    .storeFileListener((cycle, file) -> Files.deleteQuietly(file))
                    .build();
        } catch (final Exception e) {
            throw new IOException("Unable to open file: " + file, e);
        }
    }

    @Override
    public void close() throws IOException {
        if (queue != null) {
            try {
                queue.close();
                queue = null;
            } catch (final Exception e) {
                throw new IOException("Unable to close the file: " + file, e);
            }
        }
    }

}
