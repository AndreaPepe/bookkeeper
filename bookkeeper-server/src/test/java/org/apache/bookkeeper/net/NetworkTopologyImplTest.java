package org.apache.bookkeeper.net;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class NetworkTopologyImplTest {

    //SUT
    private NetworkTopologyImpl sut;

    private TestType testType;
    private Node node;

    public NetworkTopologyImplTest(TestType testType, Node node) {
        configure(testType, node);
    }

    private void configure(TestType testType, Node node) {
        this.sut = new NetworkTopologyImpl();

        this.testType = testType;
        this.node = node;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {TestType.NULL_NODE, buildNode(TestType.NULL_NODE)},
                {TestType.DATA_NODE, buildNode(TestType.DATA_NODE)},
                {TestType.INNER_NODE, buildNode(TestType.INNER_NODE)},
//                {TestType.DATA_NODE_ADDED_TO_LEAF, buildNode(TestType.DATA_NODE_ADDED_TO_LEAF)},
                {TestType.INVALID_TOPOLOGY_EXCEPTION, buildNode(TestType.DATA_NODE_ADDED_TO_LEAF)}
        });
    }

    private static Node buildNode(TestType testType) {
        Node node;
        switch (testType) {
            case DATA_NODE:
                node = new NodeBase("127.0.0.1:4000", "/root");
                break;
            case INNER_NODE:
                node = new NetworkTopologyImpl.InnerNode("127.0.0.1:4001", "/inner");
                break;
            case DATA_NODE_ADDED_TO_LEAF:
            case INVALID_TOPOLOGY_EXCEPTION:
                // the node at location /rack with name "127.0.0.1:3999" needs to be added as a DataNode base before adding this one
                node = new NodeBase("127.0.0.1:4002", "/rack/127.0.0.1:3999");
                break;
            case NULL_NODE:
            default:
                node = null;
        }
        return node;
    }

    @Test
    public void testAdd() {
        Assume.assumeTrue(testType != TestType.DATA_NODE_ADDED_TO_LEAF && testType != TestType.INVALID_TOPOLOGY_EXCEPTION);
        try {
            sut.add(node);
            // if node is not contained, but it was null, test is passed because it's right
            // that null has not been added
            boolean testPassed = sut.contains(node) || testType == TestType.NULL_NODE;
            if (testType != TestType.INNER_NODE)
                Assert.assertTrue(testPassed);
            else
                fail("Exception expected, but not thrown");
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
            Assert.assertSame(testType, TestType.INNER_NODE);
        }
    }

    @Test
    public void testAddToLeaf() {
        Assume.assumeTrue(testType == TestType.DATA_NODE_ADDED_TO_LEAF);
        sut.add(new NodeBase("127.0.0.1:3999", "/rack"));
        try {
            sut.add(node);
            fail("InvalidTopologyException Test: exception expected but not thrown");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(true);
        }
    }

    @Test
    public void testAddInvalidTopologyException() {
        Assume.assumeTrue(testType == TestType.INVALID_TOPOLOGY_EXCEPTION);
        sut.add(new NodeBase("127.0.0.1:3999", "/rack"));
        try {
            sut.add(node);
            fail("Add To Leaf Test: exception expected but not thrown");
        } catch (NetworkTopologyImpl.InvalidTopologyException e) {
            Assert.assertTrue(true);
        }
    }

    @Test
    public void testRemove() {
        Assume.assumeTrue(testType != TestType.DATA_NODE_ADDED_TO_LEAF && testType != TestType.INVALID_TOPOLOGY_EXCEPTION);
        if (!sut.contains(node) && !(node instanceof NetworkTopologyImpl.InnerNode))
            sut.add(node);
        try {
            sut.remove(node);
            Assert.assertFalse(sut.contains(node));
        } catch (IllegalArgumentException e) {
            // an inner node cannot be removed
            Assert.assertSame(TestType.INNER_NODE, testType);
        }

    }

    private enum TestType {
        NULL_NODE,
        DATA_NODE,
        DATA_NODE_ADDED_TO_LEAF,
        INNER_NODE,
        INVALID_TOPOLOGY_EXCEPTION
    }
}
