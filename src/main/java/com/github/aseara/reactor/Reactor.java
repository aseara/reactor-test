package com.github.aseara.reactor;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.util.Set;

public class Reactor implements Runnable {
    private final Selector selector;

    public Reactor(int port) throws IOException {
        selector = Selector.open();
        ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.socket().bind(new InetSocketAddress(port));
        serverSocketChannel.configureBlocking(false);
        SelectionKey sk = serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
        sk.attach(new Acceptor(serverSocketChannel, selector));
    }

    @Override
    public void run() {
        try {
            while (!Thread.interrupted()) {
                selector.select();
                Set<SelectionKey> selected = selector.selectedKeys();
                for (SelectionKey k : selected) {
                    dispatch(k);
                }
                selected.clear();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (selector != null) {
                try {
                    selector.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void dispatch(SelectionKey k) {
        Runnable r = (Runnable) (k.attachment());
        if (r != null) {
            r.run();
        }
    }

}
