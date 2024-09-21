package com.torquato.akka.bigprimes;

import akka.Done;
import akka.NotUsed;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.Behaviors;
import akka.stream.Attributes;
import akka.stream.OverflowStrategy;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import lombok.extern.slf4j.Slf4j;

import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ThreadLocalRandom;

@Slf4j
public class BigPrimesMain {

    public static final int AVAILABLE_PROCESSORS = Runtime.getRuntime().availableProcessors();

    public static void main(String[] args) {
        final Instant startTime = Instant.now();

        final Random random = ThreadLocalRandom.current();

        final Source<Integer, NotUsed> source = Source.range(0, 10);

        final Flow<Integer, BigInteger, NotUsed> convertToBigInteger = Flow.of(Integer.class)
                .map(_ -> new BigInteger(3000, random))
                .log("BigInteger");

        final Flow<BigInteger, BigInteger, NotUsed> generateProbablePrime = Flow.of(BigInteger.class)
                .map(BigInteger::nextProbablePrime)
                .log("ProbablePrime");

        final Flow<BigInteger, BigInteger, NotUsed> generateProbablePrimeParallel = Flow.of(BigInteger.class)
                .mapAsyncUnordered(AVAILABLE_PROCESSORS, (input) -> {
                    final CompletableFuture<BigInteger> future = new CompletableFuture<>();
                    return future.completeAsync(input::nextProbablePrime);
                })
                .log("ProbablePrime");

        final Flow<BigInteger, List<BigInteger>, NotUsed> groupAndSort = Flow.of(BigInteger.class)
                .grouped(10)
                .map(v -> {
                    final ArrayList<BigInteger> copy = new ArrayList<>(v);
                    copy.sort(Comparator.naturalOrder());
                    return copy;
                });


        final Sink<List<BigInteger>, CompletionStage<Done>> printSink = Sink.foreach(v -> log.info("{}", v));


        final ActorSystem<Object> actorSystem = ActorSystem.create(Behaviors.empty(), "BigPrimes");
        final CompletionStage<Done> completionStage = source
                .via(convertToBigInteger)
//                .buffer(16, OverflowStrategy.backpressure())
                .async()
                .via(generateProbablePrimeParallel)
                .async()
                .via(groupAndSort)
                .toMat(printSink, Keep.right())
                .run(actorSystem);

        completionStage.whenComplete((_, _) -> {
            long millis = Duration.between(startTime, Instant.now()).toMillis();
            log.info("Time spent {}ms", millis);
            actorSystem.terminate();
        });
    }

}
