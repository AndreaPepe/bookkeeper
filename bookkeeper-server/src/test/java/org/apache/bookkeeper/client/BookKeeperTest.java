package org.apache.bookkeeper.client;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.util.LocalBookKeeper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class BookKeeperTest {

    private LocalBookKeeper localBk;
    private BookKeeper bkc;


//    @Before
//    public void setupServer() throws Exception {
//        ServerConfiguration conf = new ServerConfiguration();
//        conf.setAllowLoopback(true);
//        this.localBk = LocalBookKeeper.getLocalBookies("127.0.0.1", 5555, 3, true, conf);
//        bkc = new BookKeeper("127.0.0.1:2181");
//    }

    @Test
    public void dummyTest() throws Exception {
        ServerConfiguration conf = new ServerConfiguration();
        conf.setAllowLoopback(true);
        this.localBk = LocalBookKeeper.getLocalBookies("127.0.0.1", 5555, 3, true, conf);
        Assert.assertTrue(true);
    }
}
