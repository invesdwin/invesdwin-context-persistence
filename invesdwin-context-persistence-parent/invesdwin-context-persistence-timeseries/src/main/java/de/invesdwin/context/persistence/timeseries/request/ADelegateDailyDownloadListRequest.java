package de.invesdwin.context.persistence.timeseries.request;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.concurrent.Callable;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.csv.writer.IBeanTableWriter;
import de.invesdwin.context.integration.network.DailyDownloadCache;
import de.invesdwin.context.integration.retry.RetryLaterRuntimeException;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.collections.list.Lists;
import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.lang.Strings;
import de.invesdwin.util.streams.pool.PooledFastByteArrayOutputStream;
import de.invesdwin.util.time.date.FDate;
import it.unimi.dsi.fastutil.io.FastByteArrayInputStream;

@NotThreadSafe
public abstract class ADelegateDailyDownloadListRequest<E> implements Callable<List<E>> {

    @Override
    public final List<E> call() throws Exception {
        try {
            final String content = newDailyDownloadCache().downloadString(getDownloadFileName(),
                    new Callable<String>() {
                        @Override
                        public String call() throws Exception {
                            try (PooledFastByteArrayOutputStream out = PooledFastByteArrayOutputStream.newInstance()) {
                                try (IBeanTableWriter<E> writer = newWriter(out.asNonClosing())) {
                                    writer.write(download());
                                }
                                return new String(out.toByteArray());
                            }
                        }
                    }, getNow());
            if (Strings.isBlank(content)) {
                throw new RetryLaterRuntimeException("Empty result for request: " + getDownloadFileName());
            }
            final ICloseableIterator<E> reader = newReader(new FastByteArrayInputStream(content.getBytes()));
            final List<E> list = Lists.toListWithoutHasNext(reader);
            Assertions.checkNotEmpty(list, "%s", getDownloadFileName());
            return list;
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
        return getDownloadName() + ".txt";
    }

    protected File getDownloadFile() {
        return DailyDownloadCache.newFile(getDownloadFileName());
    }

    protected abstract IBeanTableWriter<E> newWriter(OutputStream out) throws IOException;

    protected abstract ICloseableIterator<E> newReader(InputStream in);

    protected abstract String getDownloadName();

    protected abstract List<E> download() throws Exception;

}
