package com.amolsharma.pub_sub_queue.handler;

import com.amolsharma.pub_sub_queue.model.Message;
import com.amolsharma.pub_sub_queue.model.Topic;
import com.amolsharma.pub_sub_queue.model.TopicSubscriber;
import lombok.Getter;
import lombok.NonNull;
import lombok.SneakyThrows;

@Getter
public class SubscriberWorker implements Runnable {

    private final Topic topic;
    private final TopicSubscriber topicSubscriber;

    public SubscriberWorker(@NonNull final Topic topic,@NonNull final TopicSubscriber topicSubscriber) {
        this.topic = topic;
        this.topicSubscriber = topicSubscriber;
    }

    @SneakyThrows
    public void run(){
        synchronized (topicSubscriber){
            do{
                int currentOffset = topicSubscriber.getOffset().get();
                while(currentOffset >= topic.getMessages().size()){
                    topicSubscriber.wait();
                }
                Message message = topic.getMessages().get(currentOffset);
                topicSubscriber.getSubscriber().consume(message);

                topicSubscriber.getOffset().compareAndSet(currentOffset, currentOffset+1);

            } while(true);
        }
    }

    synchronized public void wakeUpIfNeeded(){
        synchronized (topicSubscriber){
            topicSubscriber.notify();
        }
    }

}
