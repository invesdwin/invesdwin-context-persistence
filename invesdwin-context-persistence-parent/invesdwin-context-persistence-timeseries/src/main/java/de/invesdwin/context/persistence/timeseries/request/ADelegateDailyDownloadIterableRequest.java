package de.invesdwin.context.persistence.timeseries.request;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.function.Consumer;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.csv.writer.IBeanTableWriter;
import de.invesdwin.context.integration.network.DailyDownloadCache;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.time.date.FDate;

@NotThreadSafe
public abstract class ADelegateDailyDownloadIterableRequest<E> implements ICloseableIterable<E> {

    @Override
    public ICloseableIterator<E> iterator() {
        try {
            final InputStream content = newDailyDownloadCache().downloadStream(getDownloadFileName(),
                    new Consumer<OutputStream>() {
                        @Override
                        public void accept(final OutputStream t) {
                            try (IBeanTableWriter<E> writer = newWriter(t)) {
                                writer.write(download());
                            } catch (final Exception e) {
                                throw new RuntimeException(e);
                            }
                        }
                    }, getNow());
            final ICloseableIterator<E> reader = newReader(content);
            return reader;
        } catch (final Throwable t) {
            DailyDownloadCache.delete(getDownloadFileName());
            throw Throwables.propagate(t);
        }
    }

    protected FDate getNow() {
        return new FDate();
    }

    protected DailyDownloadCache newDailyDownloadCache() {
        return new DailyDownloadCache();
    }

    protected String getDownloadFileName() {
        return getDownloadName() + ".bin.lz4";
    }

    protected File getDownloadFile() {
        return DailyDownloadCache.newFile(getDownloadFileName());
    }

    protected abstract IBeanTableWriter<E> newWriter(OutputStream out) throws IOException;

    protected abstract ICloseableIterator<E> newReader(InputStream in);

    protected abstract String getDownloadName();

    protected abstract ICloseableIterator<E> download() throws Exception;

}
