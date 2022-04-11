package com.amolsharma.pub_sub_queue.public_interface;

import com.amolsharma.pub_sub_queue.handler.TopicHandler;
import com.amolsharma.pub_sub_queue.model.Message;
import com.amolsharma.pub_sub_queue.model.Topic;
import com.amolsharma.pub_sub_queue.model.TopicSubscriber;
import lombok.NonNull;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class Queue {
    private final Map<String, TopicHandler> topicHandlers;

    public Queue(){
        this.topicHandlers = new HashMap<String, TopicHandler>();
    }

    public Topic createTopic(@NonNull final String topicName){
        final Topic topic = new Topic(topicName, UUID.randomUUID().toString());
        TopicHandler topicHandler = new TopicHandler(topic);
        topicHandlers.put(topic.getTopicId(), topicHandler);
        System.out.println("Created Topic : " + topicName);
        return topic;
    }

    public void subscribe(@NonNull final ISubscriber subscriber, @NonNull final Topic topic){
        topic.addSubscriber(new TopicSubscriber(subscriber));
        System.out.println(subscriber.getId() + " subscribed to topic " + topic.getTopicName());
    }

    public void publish(@NonNull final Topic topic, @NonNull final Message message){
        topic.addMessage(message);
        System.out.println(message.getMsg() + " published to topic " + topic.getTopicName());
        new Thread(() -> topicHandlers.get(topic.getTopicId()).publish()).start();
    }

    public void resetOffset(@NonNull final Topic topic, @NonNull final ISubscriber subscriber, @NonNull final Integer newOffset){
        for(TopicSubscriber topicSubscriber: topic.getSubscribers()){
            if(topicSubscriber.getSubscriber().equals(subscriber)){
                topicSubscriber.getOffset().set(newOffset);
                System.out.println(topicSubscriber.getSubscriber().getId() + " Offset reset to: " + newOffset);
                new Thread(() -> topicHandlers.get(topic.getTopicId()).startSubscriberWorker(topicSubscriber)).start();
                break;
            }
        }

    }

}
