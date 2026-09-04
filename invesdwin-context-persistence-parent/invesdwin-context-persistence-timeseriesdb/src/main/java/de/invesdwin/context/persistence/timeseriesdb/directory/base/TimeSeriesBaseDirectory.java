package de.invesdwin.context.persistence.timeseriesdb.directory.base;

import java.io.File;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.retry.RetryLaterRuntimeException;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.lang.Objects;

@Immutable
public class TimeSeriesBaseDirectory implements ITimeSeriesBaseDirectory {

    private final File baseDirectoryShared;
    private final File baseDirectoryPerNode;

    public TimeSeriesBaseDirectory(final File baseDirectory) {
        this(baseDirectory, baseDirectory);
    }

    public TimeSeriesBaseDirectory(final File baseDirectoryShared, final File baseDirectoryPerNode) {
        this.baseDirectoryShared = assertDirectory("baseDirectoryShared", baseDirectoryShared);
        this.baseDirectoryPerNode = assertDirectory("baseDirectoryPerNode", baseDirectoryPerNode);
    }

    private File assertDirectory(final String name, final File baseDirectory) {
        if (baseDirectory == null) {
            throw new RetryLaterRuntimeException(
                    "The " + name + " should not be null, maybe this table was already finalized?");
        }
        if (Objects.equals(baseDirectory.getAbsolutePath(), new File(".").getAbsolutePath())) {
            throw new IllegalStateException(
                    "Should not use current working directory as " + name + ": " + baseDirectory);
        }
        return baseDirectory;
    }

    @Override
    public File getBaseDirectoryShared() {
        return baseDirectoryShared;
    }

    @Override
    public File getBaseDirectoryPerNode() {
        return baseDirectoryPerNode;
    }

    @Override
    public void delete() {
        Files.deleteNative(baseDirectoryShared);
        if (!Objects.equals(baseDirectoryShared, baseDirectoryPerNode)) {
            Files.deleteNative(baseDirectoryPerNode);
        }
    }

}
