package com.github.aseara.reactor;

import java.io.IOException;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

public class Acceptor implements Runnable {

    private final Selector selector;
    private final ServerSocketChannel serverSocketChannel;

    public Acceptor(ServerSocketChannel serverSocketChannel, Selector selector) {
        this.serverSocketChannel = serverSocketChannel;
        this.selector = selector;
    }

    @Override
    public void run() {
        SocketChannel socketChannel;
        try {
            socketChannel = serverSocketChannel.accept();
            if (socketChannel != null) {
                System.out.printf("收到来自 %s 的连接%n", socketChannel.getRemoteAddress());
                new Handler(socketChannel, selector);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
