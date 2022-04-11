package com.amolsharma.pub_sub_queue.public_interface;

import com.amolsharma.pub_sub_queue.model.Message;

public interface ISubscriber {
    String getId();
    void consume(Message message) throws InterruptedException;
}
