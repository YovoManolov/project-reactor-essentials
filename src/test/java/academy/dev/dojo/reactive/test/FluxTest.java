package academy.dev.dojo.reactive.test;

import java.time.Duration;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

@Slf4j
public class FluxTest {

  @Test
  public void fluxSubscriber() {
    Flux<String> fluxString = Flux.just("William", "Suane", "DevDojo", "Academy").log();

    StepVerifier.create(fluxString)
        .expectNext("William", "Suane", "DevDojo", "Academy")
        .verifyComplete();
  }

  @Test
  public void fluxSubscriberNumbers() {
    Flux<Integer> flux = Flux.range(1, 5).log();

    flux.subscribe(i -> log.info("Number: {}", i));

    log.info("---------------------------------");

    StepVerifier.create(flux).expectNext(1, 2, 3, 4, 5).verifyComplete();
  }

    @Test
    public void fluxSubscriberNumbersError() {
        Flux<Integer> flux =
                Flux.range(1, 5)
                        .log()
                        .map(
                                i -> {
                                    if (i == 4) {
                                        throw new IndexOutOfBoundsException("Index error");
                                    }
                                    return i;
                                });

        StepVerifier.create(flux)
                .expectNext(1, 2, 3) // Validate the first three elements
                .expectErrorMatches(throwable ->
                        throwable instanceof IndexOutOfBoundsException &&
                                throwable.getMessage().equals("Index error")) // Validate the error
                .verify();
    }

  @Test
  public void fluxSubscriberNumbersUglyBackpressure() {

    Flux<Integer> flux = Flux.range(1, 10).log();

    flux.subscribe(
        new Subscriber<>() {

          private int count = 0;
          private Subscription subscription;
          private final int requestCount = 2;

          @Override
          public void onSubscribe(Subscription subscription) {
            this.subscription = subscription;
            subscription.request(requestCount);
          }

          @Override
          public void onNext(Integer integer) {
            count++;
            log.info("value {}, counter {}", integer, count);
            if (count >= requestCount) {
              this.count = 0;
              subscription.request(requestCount);
            }
          }

          @Override
          public void onError(Throwable t) {
            log.error("Error occurred", t);
          }

          @Override
          public void onComplete() {
            log.info("Completed");
          }
        });

    log.info("---------------------------------");

    StepVerifier.create(flux, 5) // Request 5 elements at a time
        .expectNext(1, 2, 3, 4, 5)
        .thenRequest(5)
        .expectNext(6, 7, 8, 9, 10)
        .verifyComplete();
  }

  @Test
  public void fluxSubscriberNumbersNotSoUglyBackpressure() {

    Flux<Integer> flux = Flux.range(1, 10).log();

    flux.subscribe(
        new BaseSubscriber<>() {

          private int count = 0;
          private final int requestCount = 2;

          @Override
          protected void hookOnSubscribe(Subscription subscription) {
            request(requestCount);
          }

          @Override
          protected void hookOnNext(Integer value) {
            count++;
            if (count >= requestCount) {
              count = 0;
              request(requestCount);
            }
          }
        });

    log.info("---------------------------------");

    StepVerifier.create(flux, 5) // Request 5 elements at a time
        .expectNext(1, 2, 3, 4, 5)
        .thenRequest(5)
        .expectNext(6, 7, 8, 9, 10)
        .verifyComplete();
  }

  @Test
  public void fluxSubscriberPrettyBackpressure() {
    Flux<Integer> flux = Flux.range(1, 10).log().limitRate(3);

    flux.subscribe(i -> log.info("Number: {}", i));

    log.info("---------------------------------");

    StepVerifier.create(flux).expectNext(1, 2, 3, 4, 5, 6, 7, 8, 9, 10).verifyComplete();
  }

  @Test
  public void fluxSubscriberIntervalOne() throws Exception {
    Flux<Long> interval = Flux.interval(Duration.ofMillis(100)).take(10).log();

    interval.subscribe(i -> log.info("Number {}", i));

    Thread.sleep(3000);
  }

  @Test
  public void fluxSubscriberIntervalTwo() {
    // uses virtual time to manipulate time passage without waiting for real time intervals
    // e.g. 1 day . This is efficient for testing long delays.
    StepVerifier.withVirtualTime(this::createInterval)
        // asserts that the subscription to the Flux happens
        .expectSubscription()
        // Simulates one dat without advancing the virtual clock
        // verifies that no events have occurred during this period
        .expectNoEvent(Duration.ofDays(1))
        // Simulates advancing the virtual clock by 1 day
        .thenAwait(Duration.ofDays(1))
        // expectNext(0L)
        .expectNext(0L)
        .thenAwait(Duration.ofDays(1))
        .expectNext(1L)
        // Simulates canceling the subscription to the Flux
        .thenCancel()
        // runs the test scenario and verifies all expectations are met
        .verify();
  }

  private Flux<Long> createInterval() {
    return Flux.interval(Duration.ofDays(1)).log();
  }

  @Test
  public void connectableFlux() {
    ConnectableFlux<Integer> connectableFlux =
        Flux.range(1, 10).log().delayElements(Duration.ofMillis(100)).publish();

    //        connectableFlux.connect();
    //
    //        //log.info("Thread sleeping for 300ms");
    //
    //        //Thread.sleep(300);
    //
    //        connectableFlux.subscribe(i -> log.info("Sub1 number {}", i));
    //
    //        //log.info("Thread sleeping for 200ms");
    //
    //        //Thread.sleep(200);
    //
    //        //connectableFlux.subscribe(i -> log.info("Sub2 number {}", i));

    StepVerifier.create(connectableFlux)
        .then(connectableFlux::connect)
        .thenConsumeWhile(i -> i <= 5)
        // 1, 2, 3, 4, 5,
        .expectNext(6, 7, 8, 9, 10)
        .expectComplete()
        .verify();
  }

  @Test
  public void connectableFluxAutoconnect() {
    Flux<Integer> fluxAutoConnect =
        Flux.range(1, 5).log().delayElements(Duration.ofMillis(100)).publish().autoConnect(2);

    // The first subscriber is the StepVerifier itself, created with
    // StepVerifier.create(fluxAutoConnect).

    // The second subscriber is explicitly added when then(fluxAutoConnect::subscribe) is called.
    // At this point, the required two subscribers are present, and the Flux starts emitting values.

    StepVerifier.create(fluxAutoConnect)
        .then(fluxAutoConnect::subscribe)
        .expectNext(1, 2, 3, 4, 5)
        .expectComplete()
        .verify();
  }
}
