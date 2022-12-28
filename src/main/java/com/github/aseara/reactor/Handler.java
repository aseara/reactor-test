package com.github.aseara.reactor;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

public class Handler implements Runnable {

    private final SelectionKey selectionKey;
    private final SocketChannel socketChannel;

    private final ByteBuffer readBuffer = ByteBuffer.allocate(1024);
    private final ByteBuffer sendBuffer = ByteBuffer.allocate(2048);

    private static final int READ = 0;
    private static final int SEND = 1;

    private int status = READ;

    public Handler(SocketChannel socketChannel, Selector selector) throws IOException {
        this.socketChannel = socketChannel;
        this.socketChannel.configureBlocking(false);

        selectionKey = socketChannel.register(selector, 0);
        selectionKey.attach(this);
        selectionKey.interestOps(SelectionKey.OP_READ);
        selector.wakeup();
    }

    @Override
    public void run() {
        try {
            switch (status) {
                case READ -> read();
                case SEND -> send();
            }
        }catch (IOException e) {
            System.err.println("read or send error: " + e.getMessage());
            selectionKey.cancel();
            try {
                socketChannel.close();
            } catch (IOException ex) {
                System.err.println("close channel error: " + ex.getMessage());
                e.printStackTrace();
            }
        }
    }

    private void read() throws IOException {
        if (selectionKey.isValid()) {
            readBuffer.clear();
            int count = socketChannel.read(readBuffer);
            if (count > 0) {
                System.out.printf("recv msg from %s: %s", socketChannel.getRemoteAddress(),
                        new String(readBuffer.array()));
                status = SEND;
                selectionKey.interestOps(SelectionKey.OP_WRITE);
            } else {
                selectionKey.cancel();
                socketChannel.close();
                System.out.println("read cause close!");
            }
        }
    }

    private void send() throws IOException {
        if (selectionKey.isValid()) {
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
            }

            status = READ;
            selectionKey.interestOps(SelectionKey.OP_READ);
        }
    }
}
