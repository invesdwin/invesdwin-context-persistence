package de.invesdwin.context.persistence.ldap.directory;

import javax.annotation.concurrent.ThreadSafe;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import de.invesdwin.context.beans.init.AMain;
import de.invesdwin.context.beans.init.MergedContext;

@ThreadSafe
public final class Main extends AMain {

    private Main(final String[] args) {
        super(args, false);
    }

    public static void main(final String[] args) {
        new Main(args);
    }

    @Override
    protected void startApplication(final CmdLineParser parser) throws CmdLineException {
        DirectoryServerContextLocation.activate();
        MergedContext.autowire(this);
        waitForShutdown();
    }
}
