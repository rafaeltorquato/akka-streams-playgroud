package com.torquato.akka.postracker;

import java.util.Date;
import java.util.Random;

public class UtilityFunctions {

    public VehiclePositionMessage getVehiclePosition(final int vehicleId) {
        //simulate some time to get a response from the vehicle
        final Random r = new Random();
        try {
            Thread.sleep(1000 * r.nextInt(5));
        } catch (InterruptedException _) {
        }

        return new VehiclePositionMessage(vehicleId, new Date(), r.nextInt(100), r.nextInt(100));
    }

    public VehicleSpeed calculateSpeed(final VehiclePositionMessage p1,
                                       final VehiclePositionMessage p2) {
        final double longDistance = Math.abs(p1.longPosition() - p2.longPosition());
        final double latDistance = Math.abs(p1.latPosition() - p2.latPosition());
        final double distanceTravelled = Math.pow((Math.pow(longDistance, 2) + Math.pow(latDistance, 2)), 0.5);
        final long time = Math.max(1, Math.abs(p1.currentDateTime().getTime() - p2.currentDateTime().getTime()) / 1000);

        double speed = distanceTravelled * 10 / time;
        if (p2.longPosition() == 0 && p2.latPosition() == 0) {
            speed = 0;
        }
        if (speed > 120) {
            speed = 50;
        }
        return new VehicleSpeed(p1.vehicleId(), speed);
    }
}
