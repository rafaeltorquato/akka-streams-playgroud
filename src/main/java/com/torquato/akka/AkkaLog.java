package com.torquato.akka;

import akka.Done;
import akka.NotUsed;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.Behaviors;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;

import java.util.concurrent.CompletionStage;

public class AkkaLog {

    public static void main(String[] args) {
        final var actorSystem = ActorSystem.create(Behaviors.empty(), "AkkaLog");

        final Source<Integer, NotUsed> source = Source.range(1, 10);

        final Flow<Integer, Integer, NotUsed> flow = Flow.of(Integer.class)
                .log("Value before")
                .map(v -> v * 2)
                .log("Value after");

        final Sink<Integer, CompletionStage<Done>> sink = Sink.foreach(System.out::println);

        source.via(flow).to(sink).run(actorSystem);

    }
}
