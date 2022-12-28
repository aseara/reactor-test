package com.github.aseara.reactor;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

public class Acceptor implements Runnable {
    private final ServerSocketChannel serverSocketChannel;

    private final int coreNum = Runtime.getRuntime().availableProcessors();
    private int next = 0;

    private final Selector[] selectors = new Selector[coreNum];
    private final SubReactor[] subReactors = new SubReactor[coreNum];
    private final Thread[] threads = new Thread[coreNum];

    public Acceptor(ServerSocketChannel serverSocketChannel) throws IOException {
        this.serverSocketChannel = serverSocketChannel;

        for (int i = 0; i < coreNum; i++) {
            selectors[i] = Selector.open();
            subReactors[i] = new SubReactor(selectors[i], i);
            threads[i] = new Thread(subReactors[i]);
            threads[i].start();
        }
    }

    @Override
    public void run() {
        SocketChannel socketChannel;
        try {
            socketChannel = serverSocketChannel.accept();
            if (socketChannel != null) {
                System.out.printf("收到来自 %s 的连接%n", socketChannel.getRemoteAddress());
                socketChannel.configureBlocking(false);

                subReactors[next].register(true);
                selectors[next].wakeup();

                SelectionKey selectionKey = socketChannel.register(selectors[next], SelectionKey.OP_READ);

                selectors[next].wakeup();
                subReactors[next].register(false);

                selectionKey.attach(new AsyncHandler(socketChannel, selectors[next]));

                if (++next == coreNum) {
                    next = 0;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
