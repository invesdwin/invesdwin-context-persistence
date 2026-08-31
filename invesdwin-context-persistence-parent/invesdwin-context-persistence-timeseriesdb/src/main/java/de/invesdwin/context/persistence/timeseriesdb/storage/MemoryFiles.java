package de.invesdwin.context.persistence.timeseriesdb.storage;

import java.io.File;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.persistence.timeseriesdb.TimeSeriesProperties;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FDates;

@Immutable
public final class MemoryFiles {

    private MemoryFiles() {}

    public static File newIncompleteMemoryFile(final File memoryFile, final long currentOffset) {
        final String timestampStr = FDate.now().toString(FDate.FORMAT_NUMBER_DATE_TIME_PS);
        final String extension = Files.getExtension(memoryFile);
        final String incompleteFilename = Files.removeExtension(memoryFile.getName()) + "_incomplete_" + currentOffset
                + "_" + timestampStr + extension;
        final File incompleteFile = new File(memoryFile.getParentFile(), incompleteFilename);
        cleanUpOldIncompleteFiles(memoryFile);
        return incompleteFile;
    }

    public static boolean isIncompleteMemoryFile(final File memoryFile) {
        return isIncompleteMemoryFile(memoryFile.getName());
    }

    public static boolean isIncompleteMemoryFile(final String memoryFileName) {
        return memoryFileName.contains("_incomplete_");
    }

    public static void cleanUpOldIncompleteFiles(final File memoryFile) {
        final File parentFile = memoryFile.getParentFile();
        if (parentFile == null || !parentFile.exists()) {
            return;
        }

        final File[] files = parentFile.listFiles((dir, name) -> isIncompleteMemoryFile(name));
        if (files == null || files.length == 0) {
            return;
        }

        final FDate[] fileDates = new FDate[files.length];

        File latestFile = null;
        FDate latestDate = FDates.MIN_DATE;

        for (int i = 0; i < files.length; i++) {
            final File f = files[i];
            final String name = f.getName();
            final String noExt = Files.removeExtension(name);
            final int lastUnderscore = noExt.lastIndexOf('_');
            if (lastUnderscore != -1) {
                final String timestampStr = noExt.substring(lastUnderscore + 1);
                try {
                    final FDate date = FDate.valueOf(timestampStr, FDate.FORMAT_NUMBER_DATE_TIME_PS);
                    fileDates[i] = date;
                    if (date.isAfterNotNullSafe(latestDate)) {
                        latestDate = date;
                        latestFile = f;
                    }
                } catch (final Exception e) {
                    // Ignore files with malformed timestamp suffixes
                }
            }
        }

        for (int i = 0; i < files.length; i++) {
            final File f = files[i];
            if (f.equals(latestFile)) {
                continue;
            } else {
                final FDate date = fileDates[i];
                if (date == null) {
                    continue;
                }
                if (TimeSeriesProperties.RETAIN_INCOMPLETE_SEGMENT_DURATION.isGreaterThan(date, latestDate)) {
                    continue;
                }
            }
            Files.deleteQuietly(f);
        }
    }

}