package org.apache.bookkeeper.client;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.util.LocalBookKeeper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

public abstract class BookKeeperTestBaseClass {
    protected LocalBookKeeper localBk;
    protected BookKeeper bkc;

    @Before
    public void setupServer() throws Exception {
        ServerConfiguration conf = new ServerConfiguration();
        conf.setAllowLoopback(true);
        localBk = LocalBookKeeper.getLocalBookies("127.0.0.1", 2181, 3, true, conf);
        localBk.start();
        bkc = new BookKeeper("127.0.0.1:2181");
    }

    @After
    public void tearDown() throws Exception {
        bkc.close();
        localBk.close();
    }
}
