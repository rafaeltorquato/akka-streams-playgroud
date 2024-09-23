package com.torquato.akka;

import akka.Done;
import akka.NotUsed;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.Behaviors;
import akka.stream.ClosedShape;
import akka.stream.FlowShape;
import akka.stream.UniformFanInShape;
import akka.stream.UniformFanOutShape;
import akka.stream.javadsl.*;

import java.util.concurrent.CompletionStage;

public class ComplexFlowTypes {

    public static void main(String[] args) {
        final ActorSystem<Object> actorSystem = ActorSystem.create(
                Behaviors.empty(),
                "ComplexFlowTypes"
        );

        final Source<Integer, NotUsed> source = Source.range(1, 1000);

        final Flow<Integer, Integer, NotUsed> oddNumbers = Flow.of(Integer.class)
                .filter(n -> n % 2 == 0)
                .log("ODD");
        final Flow<Integer, Integer, NotUsed> evenNumbers = Flow.of(Integer.class)
                .filter(n -> n % 2 == 1)
                .log("EVEN");

        final Sink<Integer, CompletionStage<Done>> print = Sink.ignore();

        RunnableGraph<CompletionStage<Done>> graph = RunnableGraph.fromGraph(
                GraphDSL.create(print, (builder, out) -> {

                    final FlowShape<Integer, Integer> evenFlow = builder.add(evenNumbers);
                    final FlowShape<Integer, Integer> oddFlow = builder.add(oddNumbers);

                    final UniformFanOutShape<Integer, Integer> broadcast = builder.add(Broadcast.create(2));
                    final UniformFanInShape<Integer, Integer> merge = builder.add(Merge.create(2));

                    builder.from(builder.add(source))
                            .viaFanOut(broadcast);

                    builder.from(broadcast)
                            .via(evenFlow);
                    builder.from(broadcast)
                            .via(oddFlow);

                    builder.from(evenFlow)
                            .viaFanIn(merge);
                    builder.from(oddFlow)
                            .viaFanIn(merge);

                    builder.from(merge)
                            .to(out);

                    return ClosedShape.getInstance();
                })
        );
        graph.run(actorSystem);

    }
}
