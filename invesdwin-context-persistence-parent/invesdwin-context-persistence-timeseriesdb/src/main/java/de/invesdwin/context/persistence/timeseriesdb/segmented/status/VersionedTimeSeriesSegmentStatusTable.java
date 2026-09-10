package de.invesdwin.context.persistence.timeseriesdb.segmented.status;

import java.io.File;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.filechannel.info.path.FileChannelPath;
import de.invesdwin.context.integration.filechannel.nio.NioFileInfo;
import de.invesdwin.context.integration.filechannel.nio.atomic.AtomicNioFileChannel;
import de.invesdwin.context.persistence.timeseriesdb.segmented.SegmentStatus;
import de.invesdwin.util.bean.tuple.ImmutableEntry;
import de.invesdwin.util.collections.factory.ILockCollectionFactory;
import de.invesdwin.util.collections.iterable.ATransformingIterator;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.collections.iterable.WrapperCloseableIterable;
import de.invesdwin.util.lang.string.Strings;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.range.TimeRange;

@ThreadSafe
public class VersionedTimeSeriesSegmentStatusTable implements ITimeSeriesSegmentStatusTable {

    private static final String DATE_FORMAT = FDate.FORMAT_NUMBER_DATE_TIME_PS;
    private static final String STATUS_EXTENSION = ".status";

    private final AtomicNioFileChannel baseChannel;
    private final int version;

    // Naturally sorted caches for high-performance iteration
    private final NavigableMap<TimeRange, SegmentStatus> terminalStatusCache = new ConcurrentSkipListMap<>(
            TimeRange.COMPARATOR);
    private final NavigableSet<TimeRange> knownRanges = new ConcurrentSkipListSet<>(TimeRange.COMPARATOR);
    private final Set<TimeRange> currentDiskRanges = ILockCollectionFactory.getInstance(false).newSet();

    private volatile FDate lastDirectoryScan = null;

    public VersionedTimeSeriesSegmentStatusTable(final File directory, final int version) {
        //CHECKSTYLE:OFF
        this(new AtomicNioFileChannel(FileChannelPath.newDirectory(directory)), version);
        //CHECKSTYLE:ON
    }

    public VersionedTimeSeriesSegmentStatusTable(final AtomicNioFileChannel baseChannel, final int version) {
        this.baseChannel = baseChannel;
        this.version = version;
    }

    public int getVersion() {
        return version;
    }

    @Override
    public SegmentStatus get(final TimeRange timeRange) {
        // 1. Fast path: terminal status already cached
        final SegmentStatus cached = terminalStatusCache.get(timeRange);
        if (cached != null) {
            return cached;
        }

        final AtomicNioFileChannel fileChannel = getChannelForRange(timeRange);

        if (!fileChannel.exists()) {
            return null; // Signals updater to initialize
        }

        try {
            final String content = fileChannel.downloadString();
            if (content == null || Strings.isBlank(content)) {
                return null;
            }
            final SegmentStatus status = SegmentStatus.valueOf(content);
            if (status.isComplete()) {
                terminalStatusCache.put(timeRange, status);
            }
            return status;
        } catch (final Exception e) {
            return null; // Corrupt/unreadable state triggers the updater
        }
    }

    @Override
    public void put(final TimeRange timeRange, final SegmentStatus status) {
        final AtomicNioFileChannel fileChannel = getChannelForRange(timeRange);
        final String name = status.name();

        // Performs a temp file creation and atomic rename.
        // Crucially, this atomic move updates the base directory's lastModified timestamp!
        fileChannel.uploadString(name);

        knownRanges.add(timeRange);

        if (status.isComplete()) {
            terminalStatusCache.put(timeRange, status);
        }
    }

    private void syncCacheWithDirectory() {
        final FDate currentModTime = baseChannel.lastModified();

        // If the directory hasn't been modified since our last scan, we can safely skip the heavy filesystem list operation
        FDate lastDirectoryScanCopy = lastDirectoryScan;
        if (lastDirectoryScanCopy != null && currentModTime != null && currentModTime.equals(lastDirectoryScanCopy)) {
            return;
        }

        synchronized (this) {
            lastDirectoryScanCopy = lastDirectoryScan;
            if (lastDirectoryScanCopy != null && currentModTime != null
                    && currentModTime.equals(lastDirectoryScanCopy)) {
                return;
            }
            if (!currentDiskRanges.isEmpty()) {
                currentDiskRanges.clear();
            }

            try (ICloseableIterator<NioFileInfo> iterator = baseChannel.listIterator()) {
                while (iterator.hasNext()) {
                    final String fileName = iterator.next().getFileName();
                    if (fileName != null && fileName.endsWith(STATUS_EXTENSION)) {
                        final TimeRange timeRange = parseRangeFromFileName(fileName);
                        if (timeRange != null) {
                            currentDiskRanges.add(timeRange);
                            knownRanges.add(timeRange);
                        }
                    }
                }
            }

            // Evict any ranges that were deleted directly from disk by other processes
            knownRanges.retainAll(currentDiskRanges);
            lastDirectoryScan = currentModTime;
        }
    }

    @Override
    public ICloseableIterator<TimeRange> rangeKeys() {
        syncCacheWithDirectory();
        return WrapperCloseableIterable.maybeWrap(knownRanges).iterator();
    }

    @Override
    public ICloseableIterator<Entry<TimeRange, SegmentStatus>> range() {
        return new ATransformingIterator<TimeRange, Entry<TimeRange, SegmentStatus>>(rangeKeys()) {
            @Override
            protected Entry<TimeRange, SegmentStatus> transform(final TimeRange value) {
                final SegmentStatus status = get(value);
                return ImmutableEntry.of(value, status);
            }
        };
    }

    @Override
    public void delete(final TimeRange segment) {
        if (segment == null) {
            return;
        }
        terminalStatusCache.remove(segment);
        knownRanges.remove(segment);
        getChannelForRange(segment).delete();
    }

    private AtomicNioFileChannel getChannelForRange(final TimeRange timeRange) {
        final String fileName = timeRange.getFrom().toString(DATE_FORMAT) + "_"
                + timeRange.getTo().toString(DATE_FORMAT) + STATUS_EXTENSION;
        return baseChannel.withFilename(fileName);
    }

    private TimeRange parseRangeFromFileName(final String fileName) {
        try {
            final String nameWithoutExt = fileName.substring(0, fileName.length() - STATUS_EXTENSION.length());
            final String[] parts = nameWithoutExt.split("_", 2);
            final FDate fromMillis = FDate.valueOf(parts[0], DATE_FORMAT);
            final FDate toMillis = FDate.valueOf(parts[1], DATE_FORMAT);
            return new TimeRange(fromMillis, toMillis);
        } catch (final Exception e) {
            return null;
        }
    }

    @Override
    public void deleteRange() {
        try (ICloseableIterator<NioFileInfo> iterator = baseChannel.listIterator()) {
            while (true) {
                final NioFileInfo info = iterator.next();
                final String fileName = info.getFileName();
                if (fileName != null && fileName.endsWith(STATUS_EXTENSION)) {
                    baseChannel.withFilename(fileName).delete();
                }
            }
        } catch (final NoSuchElementException e) {
            // End of iterator reached, nothing to do
        }
        if (!terminalStatusCache.isEmpty()) {
            terminalStatusCache.clear();
        }
        if (!knownRanges.isEmpty()) {
            knownRanges.clear();
        }
        lastDirectoryScan = null;
    }

    @Override
    public Entry<TimeRange, SegmentStatus> getLatest() {
        return getLatest(null);
    }

    @Override
    public Entry<TimeRange, SegmentStatus> getLatest(final TimeRange timeRange) {
        // 1. Ensure our list of known TimeRanges is up-to-date
        syncCacheWithDirectory();

        NavigableSet<TimeRange> searchSpace = knownRanges;

        // 2. O(log N) jump: Because COMPARATOR only evaluates getFrom(),
        // this perfectly bounds the set to ranges that start on or before 'to'.
        if (timeRange != null) {
            searchSpace = searchSpace.headSet(timeRange, true);
        }

        // 3. Iterate backwards from our optimized subset
        final Iterator<TimeRange> it = searchSpace.descendingIterator();
        try {
            final TimeRange range = it.next();
            final SegmentStatus status = get(range);
            if (status != null) {
                return ImmutableEntry.of(range, status);
            }
        } catch (final NoSuchElementException e) {
            //end reached
        }
        return null;
    }

    @Override
    public void close() {
        if (!terminalStatusCache.isEmpty()) {
            terminalStatusCache.clear();
        }
        if (!knownRanges.isEmpty()) {
            knownRanges.clear();
        }
        synchronized (this) {
            if (!currentDiskRanges.isEmpty()) {
                currentDiskRanges.clear();
            }
            lastDirectoryScan = null;
        }
    }
}