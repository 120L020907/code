package model;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class MessageQueue<MyMessage> {
    private LinkedList<MyMessage> queue;

    public MessageQueue() {
        queue = new LinkedList<MyMessage>();
    }

    public synchronized void enqueue(MyMessage message) {
        queue.addLast(message);
        notify(); // 唤醒等待的线程
    }

    public synchronized MyMessage dequeue() throws InterruptedException {
        while (queue.isEmpty()) {
            wait(); // 等待消息到来
        }
        return queue.removeFirst();
    }
    public synchronized List<MyMessage> getAllMessages() {
        List<MyMessage> messages = new ArrayList<>();
        while (!queue.isEmpty()) {
            messages.add(queue.removeFirst());
        }
        return messages;
    }
}
