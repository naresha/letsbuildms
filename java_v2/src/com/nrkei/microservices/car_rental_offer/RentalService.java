package com.nrkei.microservices.car_rental_offer;

import com.nrkei.microservices.demo.SampleService;
import com.nrkei.microservices.rapids_rivers.Packet;
import com.nrkei.microservices.rapids_rivers.PacketProblems;
import com.nrkei.microservices.rapids_rivers.RapidsConnection;
import com.nrkei.microservices.rapids_rivers.River;
import com.nrkei.microservices.rapids_rivers.rabbit_mq.RabbitMqRapids;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RentalService implements River.PacketListener {
    static RapidsConnection rapidsConnection;

    public static void main(String[] args) {
        String host = "127.0.0.1";
        String port = "5672";

        rapidsConnection = new RabbitMqRapids("rental_service", host, port);
        final River river = new River(rapidsConnection);
        // See RiverTest for various functions River supports to aid in filtering, like:
        river.requireValue("need", "car_rental_offer");  // Reject packet unless it has key:value pair
        //river.require("key1", "key2");       // Reject packet unless it has key1 and key2
        //river.forbid("key1", "key2");        // Reject packet if it does have key1 or key2
        river.forbid("offers");
        //river.interestedIn("key1", "key2");  // Allows key1 and key2 to be queried and set in a packet
        river.register(new RentalService());         // Hook up to the river to start receiving traffic
    }

    @Override
    public void packet(RapidsConnection connection, Packet packet, PacketProblems warnings) {
        System.out.println("Received: " + packet.toJson());
        Packet responsePacket = createResponsePacket(packet);
        rapidsConnection.publish(responsePacket.toJson());
    }

    private Packet createResponsePacket(Packet packet) {
        //Map<String, Object> jsonMap = new HashMap<>();
        //jsonMap.put("solution", "rental_offer");
        List<Map<String, Integer>> offers = new ArrayList<>();
        HashMap<String, Integer> offer1 = new HashMap<>();
        offer1.put("SEDAN", 300);
        offers.add(offer1);
        HashMap<String, Integer> offer2 = new HashMap<>();
        offer2.put("HATCHBACK", 250);
        offers.add(offer2);
        packet.put("offers", offers);
        //Packet responsePacket = new Packet(jsonMap);
        return packet;
    }

    @Override
    public void onError(RapidsConnection connection, PacketProblems errors) {

    }
}
