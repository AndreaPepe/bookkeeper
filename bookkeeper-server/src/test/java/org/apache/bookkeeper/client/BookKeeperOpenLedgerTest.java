package org.apache.bookkeeper.client;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class BookKeeperOpenLedgerTest extends BookKeeperTestBaseClass{

    private TestType testType;
    private BookKeeper.DigestType digestType;
    private byte[] passwd;


    public BookKeeperOpenLedgerTest(TestType testType, BookKeeper.DigestType digestType, byte[] passwd) {
        configure(testType, digestType, passwd);
    }

    private void configure(TestType testType, BookKeeper.DigestType digestType, byte[] passwd) {
        this.testType = testType;
        this.digestType = digestType;
        this.passwd = passwd;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> parameters(){
        return Arrays.asList(new Object[][]{
                {TestType.VALID_PASSWORD, BookKeeper.DigestType.MAC, "passwd".getBytes()},
                {TestType.VALID_PASSWORD, BookKeeper.DigestType.CRC32, new byte[]{}},
                {TestType.INVALID_PASSWORD, BookKeeper.DigestType.CRC32C, "passwd".getBytes()},
                {TestType.LEDGER_NOT_EXISTS, BookKeeper.DigestType.DUMMY, "passwd".getBytes()},
                {TestType.LEDGER_NOT_EXISTS, BookKeeper.DigestType.MAC, new byte[]{}},
                {TestType.LEDGER_NOT_EXISTS, BookKeeper.DigestType.CRC32, "passwd".getBytes()}
        });
    }


    @Test
    public void testOpenLedger() throws BKException, InterruptedException {

        LedgerHandle handle = null;
        if (testType == TestType.VALID_PASSWORD || testType == TestType.INVALID_PASSWORD)
            handle = bkc.createLedger(1,1,1, digestType, passwd, null);

        byte[] passwordToUse =  testType != TestType.INVALID_PASSWORD ? passwd : "wrongPassword".getBytes();

        if (handle == null){
            // ledger does not exists
            try {
                // random chosen ledger ID (<0 does not exist)
                bkc.openLedger(-1, digestType, passwordToUse);
            }catch (BKException | InterruptedException e){
                Assert.assertTrue("Ledger should not exist, bu exception has not been thrown", true);
            }
        }
        else {
            try{
                long lId = handle.getId();
                String testEntry = "entry test";
                handle.addEntry(testEntry.getBytes());
                handle.close();

                LedgerHandle newHandle = bkc.openLedger(lId, digestType, passwordToUse);
                LedgerEntry entry = newHandle.readLastEntry();
                byte[] entryContent = entry.getEntry();

                boolean entryCorrect = areEqualsByteArray(testEntry.getBytes(), entryContent);
                Assert.assertTrue(entryCorrect);
                newHandle.close();
            }catch (BKException | InterruptedException e ){
                Assert.assertSame("Exception thrown, but call to openLedger should have been valid", testType, TestType.INVALID_PASSWORD);
            }

        }
    }

    private boolean areEqualsByteArray(byte[] one, byte[] two){
        boolean ret = true;
        if (one.length != two.length)
            return false;
        for (int i = 0; i < one.length; i++){
            if (one[i] != two[i]){
                ret = false;
                break;
            }
        }

        return ret;
    }
    private enum TestType {VALID_PASSWORD, INVALID_PASSWORD, LEDGER_NOT_EXISTS}
}
