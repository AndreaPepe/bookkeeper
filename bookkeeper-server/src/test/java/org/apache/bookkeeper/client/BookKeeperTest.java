package org.apache.bookkeeper.client;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.util.LocalBookKeeper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class BookKeeperTest {

    private LocalBookKeeper localBk;
    private BookKeeper bkc;


    @Before
    public void setupServer() throws Exception {
        ServerConfiguration conf = new ServerConfiguration();
        conf.setAllowLoopback(true);
        this.localBk = LocalBookKeeper.getLocalBookies("127.0.0.1", 2181, 3, true, conf);
        this.localBk.start();
        bkc = new BookKeeper("127.0.0.1:2181");
    }

    @Test
    public void dummyTest() throws BKException, InterruptedException {
        byte[] passwd = "passwd".getBytes(StandardCharsets.UTF_8);
        LedgerHandle handle = bkc.createLedger(BookKeeper.DigestType.DUMMY, passwd);
        long numBookies = handle.getNumBookies();
        Assert.assertTrue(true);
    }

    @After
    public void tearDown() throws Exception {
        bkc.close();
        localBk.shutdownBookies();
        localBk.close();
    }
}
