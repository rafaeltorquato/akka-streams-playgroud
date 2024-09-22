package com.torquato.akka.postracker;

import akka.NotUsed;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.Behaviors;
import akka.stream.ClosedShape;
import akka.stream.javadsl.*;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

//source - repeat some value every 10 seconds.

//flow 1 - transform into the ids of each van (ie 1..8) with mapConcat

//flow 2 - get position for each van as a VPMs with a call to the lookup method (create a new instance of
//utility functions each time). Note that this process isn't instant so should be run in parallel.

//flow 3 - use previous position from the map to calculate the current speed of each vehicle. Replace the
// position in the map with the newest position and pass the current speed downstream

//flow 4 - filter to only keep those values with a speed > 95

//sink - as soon as 1 value is received return it as a materialized value, and terminate the stream
@Slf4j
public class PosTrackerMain {

    public static final int AVAILABLE_PROCESSORS = Runtime.getRuntime().availableProcessors();

    public static void main(String[] args) {

        final UtilityFunctions util = new UtilityFunctions();

        final Map<Integer, VehiclePositionMessage> vehicleTrackingMap = new HashMap<>();
        for (int i = 1; i <= 8; i++) {
            vehicleTrackingMap.put(i, new VehiclePositionMessage(i, new Date(), 0, 0));
        }

        final Source<Integer, NotUsed> source = Source
                .repeat("Go")
                .throttle(vehicleTrackingMap.size(), Duration.ofSeconds(10))
                .mapConcat((_) -> vehicleTrackingMap.keySet())
                .log("SourceOutput");

        final Flow<Integer, VehiclePositionMessage, NotUsed> getCurrentPosition = Flow
                .of(Integer.class)
                .mapAsyncUnordered(AVAILABLE_PROCESSORS, (value) -> new CompletableFuture<VehiclePositionMessage>().completeAsync(() -> util.getVehiclePosition(value)));

        final Flow<VehiclePositionMessage, VehicleSpeed, NotUsed> calculateSpeed = Flow
                .of(VehiclePositionMessage.class)
                .map((current) -> {
                    final var previous = vehicleTrackingMap.get(current.vehicleId());
                    final VehicleSpeed vehicleSpeed = util.calculateSpeed(current, previous);
                    vehicleTrackingMap.put(current.vehicleId(), current);
                    return vehicleSpeed;
                });

        final Flow<VehicleSpeed, VehicleSpeed, NotUsed> speedFilter = Flow
                .of(VehicleSpeed.class)
                .filter((vs) -> vs.speed() > 95);

        final Sink<VehicleSpeed, CompletionStage<VehicleSpeed>> getFirst = Sink
                .head();

        final ActorSystem<Object> actorSystem = ActorSystem.create(Behaviors.empty(), "PosTracker");
//        final CompletionStage<VehicleSpeed> completionStage = source
//                .async()
//                .via(getCurrentPosition)
//                .async()
//                .via(calculateSpeed)
//                .via(speedFilter)
//                .toMat(getFirst, Keep.right())
//                .run(actorSystem);

        // DSL
        final CompletionStage<VehicleSpeed> completionStage = RunnableGraph.fromGraph(
                        GraphDSL.create(getFirst, (builder, out) -> {
                            builder.from(builder.add(source))
                                    .via(builder.add(getCurrentPosition.async()))
                                    .via(builder.add(calculateSpeed))
                                    .via(builder.add(speedFilter))
                                    .to(out);
                            return ClosedShape.getInstance();
                        }))
                .run(actorSystem);

        completionStage.whenComplete((vs, _) -> {
            if (vs != null) {
                log.info("{} too fast!", vs);
            }
            actorSystem.terminate();
        });

    }

}
