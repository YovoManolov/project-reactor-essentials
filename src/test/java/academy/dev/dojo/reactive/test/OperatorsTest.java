package academy.dev.dojo.reactive.test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

@Slf4j
public class OperatorsTest {

    @Test
    public void subscribeOnSimple() {
        Flux<Integer> flux =
                Flux.range(1, 4)
                        .map(
                                i -> {
                                    log.info("Map 1 - Number {} on Thread {}", i, Thread.currentThread().getName());
                                    return i;
                                })
                        // when we apply subscribeOn it affects everything that the publisher is going to
                        // publish
                        .subscribeOn(Schedulers.single())
                        .map(
                                i -> {
                                    log.info("Map 2 - Number {} on Thread {}", i, Thread.currentThread().getName());
                                    return i;
                                });

        StepVerifier.create(flux).expectSubscription().expectNext(1, 2, 3, 4).verifyComplete();
    }

    @Test
    public void publishOnSimple() {
        Flux<Integer> flux =
                Flux.range(1, 4)
                        .map(
                                i -> {
                                    log.info("Map 1 - Number {} on Thread {}", i, Thread.currentThread().getName());
                                    return i;
                                })
                        .publishOn(Schedulers.boundedElastic())
                        .map(
                                i -> {
                                    log.info("Map 2 - Number {} on Thread {}", i, Thread.currentThread().getName());
                                    return i;
                                });
        StepVerifier.create(flux).expectSubscription().expectNext(1, 2, 3, 4).verifyComplete();
    }

    @Test
    public void multipleSubscribeOnSimple() {
        Flux<Integer> flux =
                Flux.range(1, 4)
                        .subscribeOn(Schedulers.boundedElastic())
                        .map(
                                i -> {
                                    log.info("Map 1 - Number {} on Thread {}", i, Thread.currentThread().getName());
                                    return i;
                                })
                        .subscribeOn(Schedulers.single())
                        .map(
                                i -> {
                                    log.info("Map 2 - Number {} on Thread {}", i, Thread.currentThread().getName());
                                    return i;
                                });
        StepVerifier.create(flux).expectSubscription().expectNext(1, 2, 3, 4).verifyComplete();
    }

    @Test
    public void multiplePublishOnSimple() {
        Flux<Integer> flux =
                Flux.range(1, 4)
                        .publishOn(Schedulers.single())
                        .map(
                                i -> {
                                    log.info("Map 1 - Number {} on Thread {}", i, Thread.currentThread().getName());
                                    return i;
                                })
                        // (publishOn) Typical Use Case: To switch to a different thread at a specific point in
                        // the pipeline.
                        .publishOn(Schedulers.boundedElastic())
                        .map(
                                i -> {
                                    log.info("Map 2 - Number {} on Thread {}", i, Thread.currentThread().getName());
                                    return i;
                                });
        StepVerifier.create(flux).expectSubscription().expectNext(1, 2, 3, 4).verifyComplete();
    }

    @Test
    public void publishAndSubscribeOnSimple() {
        Flux<Integer> flux =
                Flux.range(1, 4)
                        .publishOn(Schedulers.single())
                        .map(
                                i -> {
                                    log.info("Map 1 - Number {} on Thread {}", i, Thread.currentThread().getName());
                                    return i;
                                })
                        .subscribeOn(Schedulers.boundedElastic())
                        .map(
                                i -> {
                                    log.info("Map 2 - Number {} on Thread {}", i, Thread.currentThread().getName());
                                    return i;
                                });
        StepVerifier.create(flux).expectSubscription().expectNext(1, 2, 3, 4).verifyComplete();
    }

    @Test
    public void subscribeAndPublishOnSimple() {
        Flux<Integer> flux =
                Flux.range(1, 4)
                        .subscribeOn(Schedulers.single())
                        .map(
                                i -> {
                                    log.info("Map 1 - Number {} on Thread {}", i, Thread.currentThread().getName());
                                    return i;
                                })
                        .publishOn(Schedulers.boundedElastic())
                        .map(
                                i -> {
                                    log.info("Map 2 - Number {} on Thread {}", i, Thread.currentThread().getName());
                                    return i;
                                });
        StepVerifier.create(flux).expectSubscription().expectNext(1, 2, 3, 4).verifyComplete();
    }

    @Test
    public void subscribeOnIO() throws InterruptedException {
        Mono<List<String>> list =
                Mono.fromCallable(() -> Files.readAllLines(Path.of("text-file")))
                        .log()
                        .subscribeOn(Schedulers.boundedElastic());

        list.subscribe(s -> log.info("{}", s));
        Thread.sleep(2000);

        StepVerifier.create(list)
                .expectSubscription()
                .thenConsumeWhile(
                        l -> {
                            Assertions.assertFalse(l.isEmpty());
                            log.info("Size {} ", l.size());
                            return true;
                        })
                .verifyComplete();
    }

    @Test
    public void switchIfEmpty() {
        Flux<Object> flux = emptyFlux().switchIfEmpty(Flux.just("not empty anymore")).log();

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext("not empty anymore")
                .expectComplete()
                .verify();
    }

    @Test
    public void deferOperator() throws Exception {
//    Mono<Long> just =
//        // Create a new Mono that emits the specified item, which is captured at instantiation time.
//        Mono.just(System.currentTimeMillis());

        Mono<Long> defer =
                // Create a Mono provider that will supply a target Mono to subscribe to for each Subscriber
                // downstream.
                Mono.defer(() -> Mono.just(System.currentTimeMillis()));

        defer.subscribe(l -> log.info("time {}", l));
        Thread.sleep(100);
        defer.subscribe(l -> log.info("time {}", l));
        Thread.sleep(100);
        defer.subscribe(l -> log.info("time {}", l));
        Thread.sleep(100);
        defer.subscribe(l -> log.info("time {}", l));

        AtomicLong atomicLong = new AtomicLong();
        defer.subscribe(atomicLong::set);
        Assertions.assertTrue(atomicLong.get() > 0);
    }

    @Test
    public void concatOperator() {

        Flux<String> flux1 = Flux.just("a", "b");

        Flux<String> flux2 = Flux.just("c", "d");

        // Flux<String> concatFlux = Flux.concat(flux1, flux2).log();
        Flux<String> concatFlux = flux1.concatWith(flux2).log();

        StepVerifier.create(concatFlux).expectNext("a", "b", "c", "d").expectComplete().verify();
    }

    @Test
    public void concatOperatorError() {

        Flux<String> flux1 =
                Flux.just("a", "b")
                        .map(
                                s -> {
                                    if (s.equals("b")) {
                                        throw new IllegalArgumentException();
                                    }
                                    return s;
                                });

        Flux<String> flux2 = Flux.just("c", "d");

        // Flux<String> concatFlux = flux1.concatWith(flux2).log();

        Flux<String> concatFlux = Flux.concatDelayError(flux1, flux2).log();

        StepVerifier.create(concatFlux)
                .expectNext("a", "c", "d")
                .expectError(IllegalArgumentException.class)
                .verify();
    }

    @Test
    public void combineLatestOperator() {
        Flux<String> flux1 = Flux.just("a", "b");
        Flux<String> flux2 = Flux.just("c", "d");

        Flux<String> combineLatest =
                Flux.combineLatest(flux1, flux2, (s1, s2) -> s1.toUpperCase() + s2.toUpperCase()).log();

        StepVerifier.create(combineLatest)
                .expectSubscription()
                .expectNext("BC", "BD")
                .expectComplete()
                .verify();
    }

    @Test
    public void mergeOperator() throws InterruptedException {
        // concat- lazy
        // merge - eager
        Flux<String> flux1 = Flux.just("a", "b").delayElements(Duration.ofMillis(200));
        Flux<String> flux2 = Flux.just("c", "d");

        // Flux<String> mergeFlux = Flux.merge(flux1,
        // flux2).delayElements(Duration.ofMillis(200)).log();
        Flux<String> mergeFlux = flux1.mergeWith(flux2).delayElements(Duration.ofMillis(200)).log();

        mergeFlux.subscribe(log::info);

        Thread.sleep(1000);

        StepVerifier.create(mergeFlux).expectNext("c", "d", "a", "b").expectComplete().verify();
    }

    @Test
    public void mergeSequentialOperator() throws InterruptedException {
        // concat- lazy
        // merge - eager
        Flux<String> flux1 = Flux.just("a", "b").delayElements(Duration.ofMillis(200));
        Flux<String> flux2 = Flux.just("c", "d");

        Flux<String> mergeFlux =
                Flux.mergeSequential(flux1, flux2, flux1).delayElements(Duration.ofMillis(200)).log();
        mergeFlux.subscribe(log::info);

        Thread.sleep(1000);

        StepVerifier.create(mergeFlux)
                .expectNext("a", "b", "c", "d", "a", "b")
                .expectComplete()
                .verify();
    }

    @Test
    public void mergeDelayErrorOperator() throws IllegalArgumentException {
        // concat- lazy
        // merge - eager
        Flux<String> flux1 = Flux.just("a", "b")
                .map(s -> {
                    if (s.equals("b")) {
                        throw new IllegalArgumentException();
                    }
                    return s;
                }).doOnError(_ -> log.error("We could do something with this"));

        Flux<String> flux2 = Flux.just("c", "d");

        Flux<String> mergeFlux =
                Flux.mergeDelayError(1, flux1, flux2, flux1).log();

        mergeFlux.subscribe(log::info);

        StepVerifier.create(mergeFlux)
                .expectSubscription()
                .expectNext("a", "c", "d", "a")
                .expectError()
                .verify();
    }

    @Test
    public void flatMapOperator() throws Exception {

        Flux<String> flux = Flux.just("a", "b");

        Flux<String> flatFlux = flux.map(String::toUpperCase)
                .flatMap(this::findByName).log();

        flatFlux.subscribe(log::info);
        Thread.sleep(500);

        StepVerifier.create(flatFlux)
                .expectSubscription()
                .expectNext("nameB1", "nameB2", "nameA1", "nameA2")
                .verifyComplete();
    }

    @Test
    public void flatMapSequentialOperator() throws Exception {

        Flux<String> flux = Flux.just("a", "b");

        Flux<String> flatFlux = flux.map(String::toUpperCase)
                .flatMapSequential(this::findByName).log();

        flatFlux.subscribe(log::info);
        Thread.sleep(500);

        StepVerifier.create(flatFlux)
                .expectSubscription()
                .expectNext("nameA1", "nameA2", "nameB1", "nameB2")
                .verifyComplete();
    }

    public Flux<String> findByName(String name) {
        return name.equals("A") ? Flux.just("nameA1", "nameA2").delayElements(Duration.ofMillis(100)) : Flux.just("nameB1", "nameB2");
    }


    @Test
    public void zipOperator() {
        Flux<String> titleFlux = Flux.just("GrandBlue", "Baki");
        Flux<String> studioFlux = Flux.just("Zero-G", "TMS Entertainment");
        Flux<Integer> episodesFlux = Flux.just(12, 24);


        Flux<Anime> animeFlux = Flux.zip(titleFlux, studioFlux, episodesFlux)
                .flatMap(tuple -> Flux.just(new Anime(tuple.getT1(), tuple.getT2(), tuple.getT3())));

        animeFlux.subscribe(anime -> log.info(anime.toString()));

        StepVerifier.create(animeFlux).expectSubscription().expectNext(
                new Anime("GrandBlue", "Zero-G", 12),
                new Anime("Baki", "TMS Entertainment", 24)).verifyComplete();
    }

    @Test
    public void zipWithOperator() {
        Flux<String> titleFlux = Flux.just("GrandBlue", "Baki");
        Flux<Integer> episodesFlux = Flux.just(12, 24);

        Flux<Anime> animeFlux = titleFlux.zipWith(episodesFlux)
                .flatMap(tuple -> Flux.just(new Anime(tuple.getT1(), null, tuple.getT2())));

        animeFlux.subscribe(anime -> log.info(anime.toString()));

        StepVerifier.create(animeFlux).expectSubscription().expectNext(
                new Anime("GrandBlue", null, 12),
                new Anime("Baki", null, 24)).verifyComplete();

    }

    private Flux<Object> emptyFlux() {
        return Flux.empty();
    }

    @AllArgsConstructor
    @Getter
    @ToString
    @EqualsAndHashCode
    static
    class Anime {
        private String title;
        private String studio;
        private int episodes;
    }
}
