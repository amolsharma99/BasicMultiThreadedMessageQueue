package com.amolsharma.pub_sub_queue.model;

import lombok.Getter;
import lombok.NonNull;

import java.util.ArrayList;
import java.util.List;

@Getter
public class Topic {
    private final String topicName;
    private final String topicId;
    private final List<Message> messages;
    private final List<TopicSubscriber> subscribers;

    public Topic(@NonNull final String topicName, @NonNull final String topicId){
        this.topicId = topicId;
        this.topicName = topicName;
        this.messages = new ArrayList<Message>();
        this.subscribers = new ArrayList<TopicSubscriber>();
    }

    public synchronized void addMessage (@NonNull final Message message){
        messages.add(message);
    }

    public void addSubscriber(@NonNull final TopicSubscriber subscriber){
        subscribers.add(subscriber);
    }

}
