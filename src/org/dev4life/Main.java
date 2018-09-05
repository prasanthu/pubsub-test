package org.dev4life;


import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.cloud.ServiceOptions;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.gson.*;
import com.google.gson.reflect.TypeToken;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import jdk.nashorn.api.scripting.JSObject;

import java.lang.reflect.Type;
import java.util.*;

public class Main {

    // use the default project id
    private static final String PROJECT_ID = ServiceOptions.getDefaultProjectId();

    public static String convert(PurchaseInfo pinfo) {
        GsonBuilder gsonBuilder = new GsonBuilder();
        Type fooType = new TypeToken<Map<String, String>>() {}.getType();

        JsonSerializer<Map<String, String>> serializer = new JsonSerializer<Map<String, String>>() {
            @Override
            public JsonElement serialize(Map<String, String> src, Type typeOfSrc, JsonSerializationContext context) {
                JsonArray array = new JsonArray();
                src.forEach((k,v)->{
                    JsonObject property = new JsonObject();
                    property.addProperty("key", k);
                    property.addProperty("value", v);
                    array.add(property);
                });
                return array;
            }
        };

        gsonBuilder.registerTypeAdapter(fooType, serializer);
        Gson customGson = gsonBuilder.create();
        return customGson.toJson(pinfo);
    }

    /**
     * Publish messages to a topic.
     *
     * @param args topic name, number of messages
     */
    public static void main(String... args) throws Exception {
        Random rand = new Random();
        // topic id, eg. "my-topic"
        String topicId = (args.length > 1 &&  args[0] != null) ? args[0]: "purchaseInfoTestTopic";
        int messageCount = (args.length > 2 && args[1] != null) ? Integer.parseInt(args[1]) : 10;
        ProjectTopicName topicName = ProjectTopicName.of(PROJECT_ID, topicId);
        Publisher publisher = null;
        List<ApiFuture<String>> futures = new ArrayList<>();

        System.out.println("Project Id = "+ PROJECT_ID);
        System.out.println("Project topic name = "+ topicName.toString());

        try {
            // Create a publisher instance with default settings bound to the topic
            publisher = Publisher.newBuilder(topicName).build();
            Gson gson = new Gson();
            for (int i = 0; i < messageCount; i++) {
//                String message = "{\"id\": \"" + i + "\"," +
//                        "\"message\": \"Message No:" + i + "\"}" ;

//                String message = "{\"id\": \"" + i + "\"," +
//                        "\"price\": { " +
//                        "\"amount\": " + i + ", " +
//                        "\"currency\": \"NOK\"}, " +
//                        "\"sku\": \"plan.product.2\"} ";

                String message = "{\"id\": \"" + i + "\"," +
                        "\"timestamp\": \"123456789\", " +
                        "\"product\": [" +
                        "{\"key\": \"currency\", " +
                        "\"value\": \"NOK\"}, " +
                        "{\"key\": \"sku\", " +
                        "\"value\": \"plan.product.3\"}] }";


                PurchaseInfo pinfo = new PurchaseInfo();
                pinfo.id = Integer.toString(i);
                pinfo.subscriberId = "prasanth";
                pinfo.timestamp = (new Date()).toInstant().getEpochSecond();
                pinfo.id = Long.toString(pinfo.timestamp)+"-"+Integer.toString(i);
                pinfo.product = pinfo.new Product();
                pinfo.product.price = pinfo.product.new Price();
                pinfo.product.price.amount = 10;
                pinfo.product.price.currency = "NOK";
                pinfo.product.sku = "my.product."+Integer.toString(i);
                HashMap<String, String> props = new HashMap<>();
                props.put("Key1", "Value "+Integer.toString(rand.nextInt(100)));
                props.put("Key2", "Value "+Integer.toString(rand.nextInt(100)));
                props.put("Key3", "Value "+Integer.toString(rand.nextInt(100)));
                props.put("Key4", "Value "+Integer.toString(rand.nextInt(100)));
                props.put("Key5", "Value "+Integer.toString(rand.nextInt(100)));
                pinfo.product.props = props;

                message = convert(pinfo);
                System.out.println(message);

                // convert message to bytes
                ByteString data = ByteString.copyFromUtf8(message);
                PubsubMessage pubsubMessage = PubsubMessage.newBuilder()
                        .setData(data)
                        .build();

                // Schedule a message to be published. Messages are automatically batched.
                ApiFuture<String> future = publisher.publish(pubsubMessage);
                futures.add(future);
            }
        } finally {
            // Wait on any pending requests
            List<String> messageIds = ApiFutures.allAsList(futures).get();

            for (String messageId : messageIds) {
                System.out.println(messageId);
            }

            if (publisher != null) {
                // When finished with the publisher, shutdown to free up resources.
                publisher.shutdown();
            }
        }
    }
}