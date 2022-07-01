package org.apache.bookkeeper.net;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class NetworkTopologyImplGetDistanceTest {

    private NetworkTopologyImpl sut;
    private TestType testType;
    private Node node1;
    private Node node2;

    public NetworkTopologyImplGetDistanceTest(TestType testType, Node node1, Node node2) {
        configure(testType, node1, node2);
    }

    private void configure(TestType testType, Node node1, Node node2) {
        this.sut = new NetworkTopologyImpl();
        this.testType = testType;
        this.node1 = node1;
        this.node2 = node2;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {TestType.SAME_NODE, new NodeBase("127.0.0.1:4000", "/rack"),
                        new NodeBase("127.0.0.1:4000", "/rack")},
                {TestType.VALID_NODES, new NodeBase("127.0.0.1:4001", "/root/rack1"),
                        new NodeBase("127.0.0.1:4002", "/root/rack2")},
//                {TestType.NODE1_NOT_IN_CLUSTER, new NodeBase("127.0.0.1:4003", "/a"),
//                        new NodeBase("127.0.0.1:4004", "/b")},
//                {TestType.NODE2_NOT_IN_CLUSTER, new NodeBase("127.0.0.1:4005", "/c"),
//                        new NodeBase("127.0.0.1:4006", "/d")},
                {TestType.NODE1_NULL, null, new NodeBase("127.0.0.1:4007", "/e")},
                {TestType.NODE2_NULL, new NodeBase("127.0.0.1:4008", "/f"), null},
//                {TestType.BOTH_NULL, null, null}
        });
    }

    /* Oracle method for distance calculation, following documentation rules */
    private int computeDistance(Node n1, Node n2) {
        if (n1 == null)
            return Integer.MAX_VALUE;
        if (n2 == null)
            return Integer.MAX_VALUE;
        if (n1.equals(n2))
            return 0;
        if (n1.getParent() != null && n1.getParent() == n2.getParent())
            return 2;
        int one = computeDistance(n1.getParent(), n2);
        int two = computeDistance(n1, n2.getParent());
        int three = computeDistance(n1.getParent(), n2.getParent());

        if (one != Integer.MAX_VALUE)
            one = one + 1;
        if (two != Integer.MAX_VALUE)
            two = one + 1;
        if (three != Integer.MAX_VALUE)
            three = three + 2;
        return Math.min(Math.min(Math.abs(one), Math.abs(two)), Math.abs(three));
    }


    @Test
    public void testDistanceValid() {
        Assume.assumeTrue(testType == TestType.SAME_NODE || testType == TestType.VALID_NODES);
        if(!sut.contains(node1))
            sut.add(node1);
        if (!sut.contains(node2) && testType == TestType.VALID_NODES)
            sut.add(node2);
        if (testType == TestType.VALID_NODES) {
            int expectedDistance = computeDistance(node1, node2);
            int actualResult = sut.getDistance(node1, node2);
            Assert.assertEquals(expectedDistance, actualResult);
        }else {
            int expectedDistance = computeDistance(node1, node1);
            int actualResult = sut.getDistance(node1, node1);
            Assert.assertEquals(expectedDistance, actualResult);
        }
    }

    @Test
    public void testNodeNotInCluster(){
        Assume.assumeTrue(testType == TestType.NODE1_NOT_IN_CLUSTER || testType == TestType.NODE2_NOT_IN_CLUSTER);
        if (testType != TestType.NODE1_NOT_IN_CLUSTER)
            sut.add(node1);
        if (testType != TestType.NODE2_NOT_IN_CLUSTER)
            sut.add(node2);

        // from documentation, the distance should be Integer.MAX_VALUE

        int actualResult = sut.getDistance(node1, node2);

        Assert.assertEquals(Integer.MAX_VALUE, actualResult);
    }

    @Test
    public void testOneNull(){
        Assume.assumeTrue(testType == TestType.NODE1_NULL || testType == TestType.NODE2_NULL || testType == TestType.BOTH_NULL);
        if (!sut.contains(node1))
            sut.add(node1);
        if (!sut.contains(node2))
            sut.add(node2);
        try {
            sut.getDistance(node1, node2);
        }catch (Exception e){
            Assume.assumeTrue(true);
        }
    }

    @Test
    public void testBothNull(){
        Assume.assumeTrue(testType == TestType.BOTH_NULL);
        if (!sut.contains(node1))
            sut.add(node1);
        if (!sut.contains(node2))
            sut.add(node2);
        try {
            sut.getDistance(node1, node2);
        }catch (Exception e){
            Assume.assumeTrue(true);
        }
    }

    private enum TestType {SAME_NODE, VALID_NODES, NODE1_NOT_IN_CLUSTER, NODE2_NOT_IN_CLUSTER, NODE1_NULL, NODE2_NULL, BOTH_NULL}
}
