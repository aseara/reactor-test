package com.github.aseara.reactor;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class AsyncHandler implements Runnable {

    private final Selector selector;

    private final SelectionKey selectionKey;
    private final SocketChannel socketChannel;

    private final ByteBuffer readBuffer = ByteBuffer.allocate(1024);
    private final ByteBuffer sendBuffer = ByteBuffer.allocate(2048);

    private static final int READ = 0;
    private static final int SEND = 1;
    private static final int PROCESSING = 2;

    private int status = READ;

    private static final ExecutorService workers = Executors.newFixedThreadPool(5);

    public AsyncHandler(SocketChannel socketChannel, Selector selector) throws IOException {
        this.selector = selector;
        this.socketChannel = socketChannel;
        this.socketChannel.configureBlocking(false);

        selectionKey = socketChannel.register(selector, 0);
        selectionKey.attach(this);
        selectionKey.interestOps(SelectionKey.OP_READ);

        selector.wakeup();
    }

    @Override
    public void run() {
        switch (status) {
            case READ -> read();
            case SEND -> send();
        }
    }

    private void read() {
        if (selectionKey.isValid()) {
            try {
                readBuffer.clear();
                int count = socketChannel.read(readBuffer);
                if (count > 0) {
                    status = PROCESSING;
                    workers.execute(this::readWorker);
                } else {
                    selectionKey.cancel();
                    socketChannel.close();
                    System.out.println("read cause close!");
                }
            } catch (IOException e) {
                System.err.println("read error: " + e.getMessage());
                selectionKey.cancel();
                try {
                    socketChannel.close();
                } catch (IOException ex) {
                    System.err.println("close channel error: " + ex.getMessage());
                    e.printStackTrace();
                }
            }
        }
    }

    private void readWorker() {
        System.out.printf("recv msg : %s", new String(readBuffer.array()));
        status = SEND;
        selectionKey.interestOps(SelectionKey.OP_WRITE);
        selector.wakeup();
    }

    private void send() {
        if (selectionKey.isValid()) {
            status = PROCESSING;
            workers.execute(this::sendWorker);
            selectionKey.interestOps(SelectionKey.OP_READ);
        }
    }

    private void sendWorker() {
        try {
            sendBuffer.clear();
            String msg = String.format("recv msg from %s: %s,  200ok;", socketChannel.getRemoteAddress(),
                    new String(readBuffer.array()));
            sendBuffer.put(msg.getBytes());
            sendBuffer.flip();
            int count = socketChannel.write(sendBuffer);

            if (count < 0) {
                selectionKey.cancel();
                socketChannel.close();
                System.out.println("send cause close!");
            } else {
                status = READ;
            }
        } catch (IOException e) {
            System.err.println("send error: " + e.getMessage());
            selectionKey.cancel();
            try {
                socketChannel.close();
            } catch (IOException ex) {
                System.err.println("close channel error: " + ex.getMessage());
                e.printStackTrace();
            }
        }
    }
}
