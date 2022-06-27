package org.apache.bookkeeper.client;

import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.util.LocalBookKeeper;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class BookKeeperTest {

    private LocalBookKeeper localBk;
    private BookKeeper bkc;


    private int ensSize;
    private int writeQuorumSize;
    private int ackQuorumSize;
    private BookKeeper.DigestType digestType;
    private byte[] passwd;
    private Map<String, byte[]> customMetadata;

    private TestType testType;

    public BookKeeperTest(TestType testType, int ensSize, int writeQuorumSize, int ackQuorumSize, BookKeeper.DigestType digestType, byte[] passwd, Map<String, byte[]> customMetadata) {
        configure(testType, ensSize, writeQuorumSize, ackQuorumSize, digestType, passwd, customMetadata);
    }

    private void configure(TestType testType, int ensSize, int writeQuorumSize, int ackQuorumSize, BookKeeper.DigestType digestType, byte[] passwd, Map<String, byte[]> customMetadata) {
        this.testType = testType;
        this.ensSize = ensSize;
        this.writeQuorumSize = writeQuorumSize;
        this.ackQuorumSize = ackQuorumSize;
        this.digestType = digestType;
        this.passwd = passwd;
        this.customMetadata = customMetadata;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> parameters() {
        Map<String, byte[]> nonEmptyMetadata = new HashMap<>();
        nonEmptyMetadata.put("myMetadata", "MyCustomMetadata".getBytes());

        return Arrays.asList(new Object[][]{
                {TestType.CREATE_VALID, 3, 2, 1, BookKeeper.DigestType.MAC, "passwd".getBytes(), nonEmptyMetadata},
                {TestType.CREATE_VALID, 2, 2, 1, BookKeeper.DigestType.CRC32, "passwd".getBytes(), null},
                {TestType.CREATE_INVALID, 1, 2, 1, BookKeeper.DigestType.MAC, new byte[]{}, null}

//                {TestType.CREATE_VALID, 2, 1, 1, BookKeeper.DigestType.MAC, "passwd".getBytes(), null},
//                {TestType.CREATE_VALID, 1, 1, 1, BookKeeper.DigestType.CRC32, new byte[]{}, new HashMap<String, byte[]>()},
//                {TestType.CREATE_INVALID, 0, 1, 1, BookKeeper.DigestType.MAC, "passwd".getBytes(), null},
//
//                {TestType.CREATE_VALID, 1, 0, 1, BookKeeper.DigestType.MAC, "passwd".getBytes(), null},
//                {TestType.CREATE_VALID, 0, 0, 1, BookKeeper.DigestType.CRC32, "passwd".getBytes(), null},
//                {TestType.CREATE_INVALID, -1, 0, 1, BookKeeper.DigestType.MAC, "passwd".getBytes(), null},
//
//                {TestType.CREATE_VALID, 2, 1, 0, BookKeeper.DigestType.MAC, new byte[]{}, new HashMap<String, byte[]>()},
//                {TestType.CREATE_VALID, 1, 1, 0, BookKeeper.DigestType.MAC, "passwd".getBytes(), null},
//                {TestType.CREATE_INVALID, 0, 1, 0, BookKeeper.DigestType.MAC, "passwd".getBytes(), null},
//
//                {TestType.CREATE_VALID, 1, 0, 0, BookKeeper.DigestType.MAC, "passwd".getBytes(), null},
//                {TestType.CREATE_INVALID, 0, 0, 0, BookKeeper.DigestType.MAC, "passwd".getBytes(), nonEmptyMetadata},
//                {TestType.CREATE_INVALID, -1, 0, 0, BookKeeper.DigestType.MAC, new byte[]{}, null},
//
//                {TestType.CREATE_INVALID, 0, -1, 0, BookKeeper.DigestType.MAC, "passwd".getBytes(), null},
//                {TestType.CREATE_INVALID, -1, -1, 0, BookKeeper.DigestType.MAC, "passwd".getBytes(), null},
//                {TestType.CREATE_INVALID, -2, -1, 0, BookKeeper.DigestType.MAC, new byte[]{}, null},
//
//                {TestType.CREATE_INVALID, 1, 0, -1, BookKeeper.DigestType.MAC, "passwd".getBytes(), null},
//                {TestType.CREATE_INVALID, 0, 0, -1, BookKeeper.DigestType.MAC, "passwd".getBytes(), nonEmptyMetadata},
//                {TestType.CREATE_INVALID, -1, 0, -1, BookKeeper.DigestType.MAC, "passwd".getBytes(), null},
//
//                {TestType.CREATE_INVALID, 0, -1, -1, BookKeeper.DigestType.MAC, "passwd".getBytes(), null},
//                {TestType.CREATE_INVALID, -1, -1, -1, BookKeeper.DigestType.MAC, new byte[]{}, null},
//                {TestType.CREATE_INVALID, -2, -1, -1, BookKeeper.DigestType.MAC, "passwd".getBytes(), null},
//
//                {TestType.CREATE_INVALID, -1, -2, -1, BookKeeper.DigestType.MAC, "passwd".getBytes(), null},
//                {TestType.CREATE_INVALID, -2, -2, -1, BookKeeper.DigestType.MAC, "passwd".getBytes(), new HashMap<String, byte[]>()},
//                {TestType.CREATE_INVALID, -3, -2, -1, BookKeeper.DigestType.MAC, "passwd".getBytes(), null}
        });
    }

    @Before
    public void setupServer() throws Exception {
        ServerConfiguration conf = new ServerConfiguration();
        conf.setAllowLoopback(true);
        this.localBk = LocalBookKeeper.getLocalBookies("127.0.0.1", 2181, 3, true, conf);
        this.localBk.start();
        bkc = new BookKeeper("127.0.0.1:2181");
    }

    @Test
    public void testCreateLedger() {
        Assume.assumeTrue(testType == TestType.CREATE_VALID);
        try {
            LedgerHandle handle = bkc.createLedger(ensSize, writeQuorumSize, ackQuorumSize, digestType, passwd, customMetadata);
            LedgerMetadata ledgerMetadata = handle.getLedgerMetadata();
            boolean correctlyConfigured = checkLedgerMetadata(ledgerMetadata);

            Assert.assertTrue(correctlyConfigured);
        } catch (BKException | InterruptedException e) {
            // test failed
            fail();
        }

    }

    @Test
    public void testCreateLedgerInvalid(){
        Assume.assumeTrue(testType == TestType.CREATE_INVALID);
        try {
            LedgerHandle handle = bkc.createLedger(ensSize, writeQuorumSize, ackQuorumSize, digestType, passwd, customMetadata);
            fail();
        } catch (BKException | IllegalArgumentException e) {
            // exception verified, test passed
            Assert.assertTrue(true);
        } catch (InterruptedException e) {
            // this exception makes the test fail
            fail();
        }
    }

    @After
    public void tearDown() throws Exception {
        bkc.close();
//        localBk.shutdownBookies();
        localBk.close();
    }


    private boolean checkLedgerMetadata(LedgerMetadata metadata) {
        if (metadata.getEnsembleSize() != ensSize)
            return false;
        if (metadata.getWriteQuorumSize() != writeQuorumSize)
            return false;
        if (metadata.getAckQuorumSize() != ackQuorumSize)
            return false;
        if (metadata.getCustomMetadata().equals(customMetadata))
            return false;


        if (metadata.getPassword().length != passwd.length)
            return false;
        boolean passwordOk = true;
        byte[] metadataPassword = metadata.getPassword();
        for (int i = 0; i < metadataPassword.length; i++){
            if (metadataPassword[i] != passwd[i]){
                passwordOk = false;
                break;
            }
        }

        return passwordOk;
    }

    private enum TestType {
        CREATE_VALID,
        CREATE_INVALID
    }
}
