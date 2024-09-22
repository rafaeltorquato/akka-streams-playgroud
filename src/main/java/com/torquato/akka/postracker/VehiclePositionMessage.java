package com.torquato.akka.postracker;

import java.util.Date;

public record VehiclePositionMessage(int vehicleId,
                                     Date currentDateTime,
                                     int longPosition,
                                     int latPosition) {
}