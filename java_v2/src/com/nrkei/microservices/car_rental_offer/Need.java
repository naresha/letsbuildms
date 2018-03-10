package com.nrkei.microservices.car_rental_offer;
/*
 * Copyright (c) 2016 by Fred George
 * May be used freely except for training; license required for training.
 * @author Fred George
 */

import com.nrkei.microservices.rapids_rivers.*;
import com.nrkei.microservices.rapids_rivers.rabbit_mq.RabbitMqRapids;

import java.util.HashMap;
import java.util.Random;

// Understands the requirement for advertising on a site
public class Need {

    public static void main(String[] args) {
        String host = "127.0.0.1";
        String port = "5672";

        final RapidsConnection rapidsConnection = new RabbitMqRapids("car_rental_need_java", host, port);
        publish(rapidsConnection);
    }

    public static void publish(RapidsConnection rapidsConnection) {
        try {
            while (true) {
                String jsonMessage = needPacket().toJson();
                System.out.println(String.format(" [<] %s", jsonMessage));
                rapidsConnection.publish(jsonMessage);
                Thread.sleep(10000);
            }
        } catch (Exception e) {
            throw new RuntimeException("Could not publish message:", e);
        }
    }

    private static Packet needPacket() {
        HashMap<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("need", "car_rental_offer");
        jsonMap.put("correlationId", String.valueOf(System.currentTimeMillis()));
        jsonMap.put("location", "BLR");
        if (new Random().nextInt() % 2 == 0) {
            jsonMap.put("userType", "PLT");
        }
        return new Packet(jsonMap);
    }
}
