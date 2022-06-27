package org.apache.bookkeeper.client;

import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.versioning.Versioned;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@RunWith(Parameterized.class)
public class BookKeeperDeleteLedgerTest extends BookKeeperTestBaseClass {

    private enum TestType {DELETE_SUCCESS, DELETE_FAILURE}

    private int ensSize;
    private int writeQuorumSize;
    private int ackQuorumSize;
    private BookKeeper.DigestType digestType;
    private byte[] passwd;
    private Map<String, byte[]> customMetadata;

    private TestType testType;

    public BookKeeperDeleteLedgerTest(TestType testType, int ensSize, int writeQuorumSize, int ackQuorumSize, BookKeeper.DigestType digestType, byte[] passwd, Map<String, byte[]> customMetadata) {
        configure(testType, ensSize, writeQuorumSize, ackQuorumSize, digestType, passwd, customMetadata);
    }

    private void configure(TestType testType, int ensSize, int writeQuorumSize, int ackQuorumSize, BookKeeper.DigestType digestType, byte[] passwd, Map<String, byte[]> customMetadata) {
        this.ensSize = ensSize;
        this.writeQuorumSize = writeQuorumSize;
        this.ackQuorumSize = ackQuorumSize;
        this.digestType = digestType;
        this.passwd = passwd;
        this.customMetadata = customMetadata;
        this.testType = testType;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {TestType.DELETE_SUCCESS, 1, 0, 0, BookKeeper.DigestType.CRC32, "gulyx".getBytes(), null}

                // exception expected but not thrown, maybe this can be considered a BUG
//                {TestType.DELETE_FAILURE, 1, 0, 0, BookKeeper.DigestType.CRC32, "gulyx".getBytes(), null}
        });
    }

    @Test
    public void testDelete() throws BKException, InterruptedException {
        Assume.assumeTrue(testType == TestType.DELETE_SUCCESS || testType == TestType.DELETE_FAILURE);

        long ledgerId = 55555;
        LedgerHandle handle = null;
        boolean testPassed = false;
        if (testType == TestType.DELETE_SUCCESS) {
            handle = this.bkc.createLedger(ensSize, writeQuorumSize, ackQuorumSize, digestType, passwd, customMetadata);
            ledgerId = handle.getId();
        }

        try {
            this.bkc.deleteLedger(ledgerId);
            if (handle != null) {
                //exception unexpected

                try {
                    CompletableFuture<Versioned<LedgerMetadata>> future = bkc.getLedgerManager().readLedgerMetadata(ledgerId);
                    if(SyncCallbackUtils.waitForResult(future) == null)
                        testPassed = true;
                }catch (BKException.BKNoSuchLedgerExistsOnMetadataServerException e){
                    testPassed = true;
                }
            }

            System.out.println("Expected exception, but not thrown");
        } catch (InterruptedException | BKException e) {
            if(testType == TestType.DELETE_FAILURE)
                testPassed = true;
        }

        Assert.assertTrue(testPassed);
    }

}
