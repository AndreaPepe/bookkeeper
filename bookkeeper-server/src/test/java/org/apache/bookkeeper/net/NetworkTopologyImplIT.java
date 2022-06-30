package org.apache.bookkeeper.net;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collection;

import static org.mockito.ArgumentMatchers.any;

/**
 * This Integration Test aims to verify if the add(Node)
 * method of the NetworkTopologyImpl class effectively acts
 * on the passed Node instance.
 * <p>
 * In particular, the expected behaviour is that the method
 * setParent() of the Node instance is invoked
 * exactly once.
 */

@RunWith(Parameterized.class)
public class NetworkTopologyImplIT {
    // SUT
    private NetworkTopologyImpl sut;

    private Node node;

    public NetworkTopologyImplIT(Node node) {
        configure(node);
    }

    public void configure(Node node) {
        this.sut = new NetworkTopologyImpl();
        this.node = node;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {new NodeBase("127.0.0.1:9999", "/rack")}
        });
    }

    @Test
    public void testAddIT() {
        node = Mockito.spy(node);
        sut.add(node);

        Mockito.verify(node, Mockito.times(1)).setParent(any());
    }
}
