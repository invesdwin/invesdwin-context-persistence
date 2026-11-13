package de.invesdwin.context.persistence.timeseriesdb.updater;

import java.io.File;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.persistence.timeseriesdb.TimeSeriesLookupStorageCache;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.lang.string.Charsets;

/**
 * The update-finished marker is how a later process of the same key adopts a finished update, so close() must publish
 * it whether or not the progress file it renames is still there. The progress file can be gone because another process
 * adopted it, or because the key's version directory was cleaned under a closed cache.
 */
@NotThreadSafe
public class TimeSeriesUpdaterResultCloseTest extends ATest {

    private File baseDirectory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        baseDirectory = new File(ContextProperties.TEMP_DIRECTORY,
                TimeSeriesUpdaterResultCloseTest.class.getSimpleName());
        Files.deleteQuietly(baseDirectory);
        Files.forceMkdir(baseDirectory);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        Files.deleteQuietly(baseDirectory);
    }

    /**
     * The marker move throws when its source is already gone, and that is not an update failure: the finished marker
     * has to be created anyway, or every later demand of this key waits for an update that already finished.
     */
    @Test
    public void testCloseStillPublishesTheFinishedMarkerWhenTheProgressFileIsGone() throws Exception {
        final File progressFile = new File(baseDirectory, TimeSeriesLookupStorageCache.UPDATE_PROGRESS_FILE_NAME);
        final File finishedFile = new File(baseDirectory, TimeSeriesLookupStorageCache.UPDATE_FINISHED_FILE_NAME);
        Assertions.assertThat(progressFile).doesNotExist();

        final TimeSeriesUpdaterResult result = new TimeSeriesUpdaterResult(null, null, progressFile, finishedFile);
        result.close();

        Assertions.assertThat(finishedFile).as("the adoption marker must exist").exists();
        Assertions.assertThat(progressFile).doesNotExist();
    }

    /**
     * The normal path is a RENAME and not a touch: the marker content is the adopted progress, so a test that only
     * checks existence would also pass on the fallback branch above.
     */
    @Test
    public void testCloseMovesTheProgressContentIntoTheFinishedMarker() throws Exception {
        final File progressFile = new File(baseDirectory, TimeSeriesLookupStorageCache.UPDATE_PROGRESS_FILE_NAME);
        final File finishedFile = new File(baseDirectory, TimeSeriesLookupStorageCache.UPDATE_FINISHED_FILE_NAME);
        Files.writeStringToFile(progressFile, "adopted-progress", Charsets.defaultCharset());

        final TimeSeriesUpdaterResult result = new TimeSeriesUpdaterResult(null, null, progressFile, finishedFile);
        result.close();

        Assertions.assertThat(progressFile).as("the progress file is renamed, not copied").doesNotExist();
        Assertions.assertThat(Files.readFileToString(finishedFile, Charsets.defaultCharset()))
                .as("the adopted progress content must be the marker content").isEqualTo("adopted-progress");
    }

    /**
     * A second close is a no-op: it must not re-create the marker it already moved away.
     */
    @Test
    public void testSecondCloseChangesNothing() throws Exception {
        final File progressFile = new File(baseDirectory, TimeSeriesLookupStorageCache.UPDATE_PROGRESS_FILE_NAME);
        final File finishedFile = new File(baseDirectory, TimeSeriesLookupStorageCache.UPDATE_FINISHED_FILE_NAME);
        Files.writeStringToFile(progressFile, "adopted-progress", Charsets.defaultCharset());

        final TimeSeriesUpdaterResult result = new TimeSeriesUpdaterResult(null, null, progressFile, finishedFile);
        result.close();
        Assertions.assertThat(finishedFile.delete()).isTrue();
        result.close();

        Assertions.assertThat(finishedFile).doesNotExist();
    }
}
