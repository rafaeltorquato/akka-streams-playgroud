package com.torquato.akka;

import akka.Done;
import akka.NotUsed;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.Behaviors;
import akka.stream.ClosedShape;
import akka.stream.UniformFanInShape;
import akka.stream.UniformFanOutShape;
import akka.stream.javadsl.*;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletionStage;

@Slf4j
public class ExploringParallelism {

    public static final int AVAILABLE_PROCESSORS = Runtime.getRuntime().availableProcessors();

    public static void main(String[] args) {
        final ActorSystem<Integer> actorSystem = ActorSystem.create(
                Behaviors.empty(),
                "ExploringParallelism"
        );

        final Source<Integer, NotUsed> source = Source.range(1, 10);

        final Flow<Integer, Integer, NotUsed> flow = Flow.of(Integer.class)
                .map(n -> {
                    Thread.sleep(3000);
                    return n;
                });

        final Sink<Integer, CompletionStage<Done>> sink = Sink.foreach(System.out::println);

        final Instant startTime = Instant.now();

        final RunnableGraph<CompletionStage<Done>> graph = RunnableGraph.fromGraph(
                GraphDSL.create(sink, (builder, out) -> {
                    final UniformFanOutShape<Integer, Integer> balance = builder.add(Balance.create(
                            AVAILABLE_PROCESSORS,
                            true
                    ));
                    final UniformFanInShape<Integer, Integer> merge = builder.add(Merge.create(AVAILABLE_PROCESSORS));

                    builder.from(builder.add(source))
                            .viaFanOut(balance);

                    for(int i = 0; i < AVAILABLE_PROCESSORS; i++) {
                        builder.from(balance)
                                .via(builder.add(flow.async()))
                                .toFanIn(merge);
                    }
                    builder.from(merge)
                            .to(out);

                    return ClosedShape.getInstance();
                })
        );
        CompletionStage<Done> completionStage = graph.run(actorSystem);
        completionStage.whenComplete((_, _) -> {
            final long millis = Duration.between(startTime, Instant.now()).toMillis();
            log.info("Time spent {} ms", millis);
            actorSystem.terminate();
        });


    }
}
