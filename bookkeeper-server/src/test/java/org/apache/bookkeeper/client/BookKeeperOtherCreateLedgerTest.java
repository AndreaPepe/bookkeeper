package org.apache.bookkeeper.client;

import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.*;

import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class BookKeeperOtherCreateLedgerTest extends BookKeeperTestBaseClass {

    private TestType testType;

    private int ensSize;
    private int qSize;
    private BookKeeper.DigestType digestType;
    private byte[] passwd;


    public BookKeeperOtherCreateLedgerTest(TestType testType, int ensSize, int qSize, BookKeeper.DigestType digestType, byte[] passwd) {
        configure(testType, ensSize, qSize, digestType, passwd);
    }

    private void configure(TestType testType, int ensSize, int qSize, BookKeeper.DigestType digestType, byte[] passwd) {
        this.testType = testType;
        this.ensSize = ensSize;
        this.qSize = qSize;
        this.digestType = digestType;
        this.passwd = passwd;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
//                {TestType.CREATE_INVALID, -2, -1, BookKeeper.DigestType.MAC, new byte[]{}},
                {TestType.CREATE_INVALID, -1, -1, BookKeeper.DigestType.CRC32, "passwd".getBytes()},
                {TestType.CREATE_VALID, 0, -1, BookKeeper.DigestType.CRC32C, "passwd".getBytes()},
//                {TestType.CREATE_INVALID, -1, 0, BookKeeper.DigestType.DUMMY, "passwd".getBytes()},
                {TestType.CREATE_VALID, 0, 0, BookKeeper.DigestType.MAC, new byte[]{}},
                {TestType.CREATE_VALID, 1, 0, BookKeeper.DigestType.MAC, "passwd".getBytes()},
//                {TestType.CREATE_INVALID, 0, 1, BookKeeper.DigestType.MAC, "passwd".getBytes()},
                {TestType.CREATE_VALID, 1, 1, BookKeeper.DigestType.MAC, "passwd".getBytes()},
                {TestType.CREATE_VALID, 2, 1, BookKeeper.DigestType.MAC, "passwd".getBytes()}
        });
    }


    @Test
    public void testCreateFourParamsValid() {
        Assume.assumeTrue(testType == TestType.CREATE_VALID);
        try {
            LedgerHandle handle = bkc.createLedger(ensSize, qSize, digestType, passwd);
            LedgerMetadata ledgerMetadata = handle.getLedgerMetadata();
            boolean correctlyConfigured = checkLedgerMetadata(ledgerMetadata, CheckType.VALID_4_PARAMS);

            Assert.assertTrue(correctlyConfigured);
        } catch (BKException | InterruptedException e) {
            // test failed
            fail();
        }
    }


    @Test
    public void testCreateFourParamsInvalid() {
        Assume.assumeTrue(testType == TestType.CREATE_INVALID);
        try {
            LedgerHandle handle = bkc.createLedger(ensSize, qSize, digestType, passwd);
            fail();
        } catch (BKException | IllegalArgumentException e) {
            // exception verified, test passed
            Assert.assertTrue(true);
        } catch (InterruptedException e) {
            // this exception makes the test fail
            fail();
        }
    }


    // the method with 2 params can't be invalid
    @Test
    public void testCreateTwoParamsValid() {
        Assume.assumeTrue(testType == TestType.CREATE_VALID);
        try {
            LedgerHandle handle = bkc.createLedger(digestType, passwd);
            LedgerMetadata ledgerMetadata = handle.getLedgerMetadata();
            boolean correctlyConfigured = checkLedgerMetadata(ledgerMetadata, CheckType.VALID_2_PARAMS);

            Assert.assertTrue(correctlyConfigured);
        } catch (BKException | InterruptedException e) {
            // test failed
            fail();
        }
    }

    private boolean checkLedgerMetadata(LedgerMetadata metadata, CheckType checkType) {
        switch (checkType) {
            case VALID_4_PARAMS:
                if (metadata.getEnsembleSize() != ensSize)
                    return false;
                if (metadata.getWriteQuorumSize() != qSize)
                    return false;
                if (metadata.getAckQuorumSize() != qSize)
                    return false;
                return areEqualsByteArray(metadata.getPassword(), passwd);

            case VALID_2_PARAMS:
                // default of 3 servers and quorum of 2 servers, from Doc
                if (metadata.getEnsembleSize() != 3)
                    return false;
                if (metadata.getWriteQuorumSize() != 2)
                    return false;
                if (metadata.getAckQuorumSize() != 2)
                    return false;
                return areEqualsByteArray(metadata.getPassword(), passwd);
        }

        //by default return false
        return false;
    }

    private boolean areEqualsByteArray(byte[] one, byte[] two) {
        boolean ret = true;
        if (one.length != two.length)
            return false;
        for (int i = 0; i < one.length; i++) {
            if (one[i] != two[i]) {
                ret = false;
                break;
            }
        }

        return ret;
    }

    private enum CheckType {
        VALID_4_PARAMS,
        VALID_2_PARAMS
    }

    private enum TestType {
        CREATE_VALID,
        CREATE_INVALID
    }
}




