package org.apache.bookkeeper.net;

import com.beust.jcommander.Parameters;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.fail;
@Ignore
@RunWith(Parameterized.class)
public class NetworkTopologyImplTest {

    //SUT
    private NetworkTopologyImpl sut;

    private TestType testType;
    private Node node;

    public NetworkTopologyImplTest(TestType testType, Node node){
        configure(testType, node);
    }

    private void configure(TestType testType, Node node){
        this.sut = new NetworkTopologyImpl();

        this.testType = testType;
        this.node = node;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> parameters(){
        return Arrays.asList(new Object[][]{
                {TestType.NULL_NODE, buildNode(TestType.NULL_NODE)},
                {TestType.VALID_NODE, buildNode(TestType.VALID_NODE)},
                {TestType.INVALID_NODE, buildNode(TestType.INVALID_NODE)}
        });
    }

    private static Node buildNode(TestType testType){
        Node node;
        switch (testType){
            case VALID_NODE:
                node = new NodeBase("127.0.0.1:4000", "/root");
                break;
            case INVALID_NODE:
                node = new NodeBase("myNameIsWrong", "/");
                break;
            case NULL_NODE:
            default:
                node = null;
        }
        return node;
    }

    @Test
    public void testAdd(){
        try{
            sut.add(node);
            boolean testPassed = sut.contains(node);
            if (testType != TestType.INVALID_NODE)
                Assert.assertTrue(testType == TestType.NULL_NODE || testPassed);
            else
                fail("Exception expected, but not thrown");
        }catch (IllegalArgumentException e){
            e.printStackTrace();
            Assert.assertSame(testType, TestType.INVALID_NODE);
        }
    }

    @Test
    public void testRemove(){
        Assume.assumeTrue(testType != TestType.INVALID_NODE);
        if (!sut.contains(node))
            sut.add(node);
        sut.remove(node);
        Assert.assertFalse(sut.contains(node));
    }
    private enum TestType {
        NULL_NODE,
        VALID_NODE,
        INVALID_NODE
    }
}
