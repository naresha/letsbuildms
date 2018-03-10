package com.nrkei.microservices.car_rental_offer;

import com.nrkei.microservices.rapids_rivers.Packet;
import com.nrkei.microservices.rapids_rivers.PacketProblems;
import com.nrkei.microservices.rapids_rivers.RapidsConnection;
import com.nrkei.microservices.rapids_rivers.River;
import com.nrkei.microservices.rapids_rivers.rabbit_mq.RabbitMqRapids;

public class DiscountService implements River.PacketListener {

    static RapidsConnection rapidsConnection;

    public static void main(String[] args) {
        String host = "127.0.0.1";
        String port = "5672";

        rapidsConnection = new RabbitMqRapids("discount_service", host, port);
        final River river = new River(rapidsConnection);
        // See RiverTest for various functions River supports to aid in filtering, like:
        river.requireValue("need", "car_rental_offer");  // Reject packet unless it has key:value pair
        //river.require("key1", "key2");       // Reject packet unless it has key1 and key2
        //river.forbid("key1", "key2");        // Reject packet if it does have key1 or key2
        river.forbid("discount");
        //river.interestedIn("key1", "key2");  // Allows key1 and key2 to be queried and set in a packet
        river.interestedIn("userType");
        river.register(new DiscountService());         // Hook up to the river to start receiving traffic
    }

    @Override
    public void packet(RapidsConnection connection, Packet packet, PacketProblems warnings) {
        System.out.println("Received: " + packet.toJson());
        String userType = (String) packet.get("userType");
        System.out.println("User Type: " + userType);
        if (userType != null && userType.equals("PLT")) {
            packet.put("discount", 10);
            rapidsConnection.publish(packet.toJson());
        }
    }

    @Override
    public void onError(RapidsConnection connection, PacketProblems errors) {

    }
}
