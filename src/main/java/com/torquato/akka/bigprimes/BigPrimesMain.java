package com.torquato.akka.bigprimes;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ThreadLocalRandom;

import akka.Done;
import akka.NotUsed;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.Behaviors;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.RunnableGraph;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BigPrimesMain {

    public static void main(String[] args) {
        final Random random = ThreadLocalRandom.current();

        final Source<Integer, NotUsed> source = Source.range(0, 9);

        final Flow<Integer, BigInteger, NotUsed> convertToBigInteger = Flow.of(Integer.class)
                .map(i -> new BigInteger(2000, random));

        final Flow<BigInteger, BigInteger, NotUsed> generateProbablePrime = Flow.of(BigInteger.class)
                .map(BigInteger::nextProbablePrime);

        final Flow<BigInteger, List<BigInteger>, NotUsed> groupAndSort = Flow.of(BigInteger.class)
                .grouped(10)
                .map(v -> {
                    final ArrayList<BigInteger> copy = new ArrayList<>(v);
                    copy.sort(Comparator.naturalOrder());
                    return copy;
                });


        final Sink<List<BigInteger>, CompletionStage<Done>> printSink = Sink.foreach(v -> log.info("{}", v));

        final RunnableGraph<NotUsed> runnableGraph = source
                .via(convertToBigInteger)
                .via(generateProbablePrime)
                .via(groupAndSort)
                .to(printSink);

        final ActorSystem<Object> actorSystem = ActorSystem.create(Behaviors.empty(), "BigPrimes");

        runnableGraph.run(actorSystem);
    }

}
