package academy.dev.dojo.reactive.test;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@Slf4j

/**
 *
 * Reactive Streams
 * 1. Asynchronous
 * 2. Non-blocking
 * 3. Backpressure
 * Publisher <-(subscribe) Subscriber
 * Subscription is created
 * Publisher (onSubscribe with the subscription) -> Subscriber
 * Subscription <- (request N) Subscriber
 * Publisher -> (onNext) Subscriber
 * until:
 * 1. Publisher sends all the objects requested
 * 2. Publisher sends all the objects it has. (onComplete) subscriber and subscription will be canceled
 * 3. There is an error. (onError) -> subscriber and subscription will be canceled
 */

public class MonoTest {

    @Test
    public void monoSubscriber(){
        String name = "William Suane";
        Mono<String> mono = Mono.just(name).log();

        mono.subscribe();

        log.info("----------------------------------");

        StepVerifier.create(mono).expectNext(name).verifyComplete();

        log.info("Mono {}",mono);
        log.info("Everything working as intended");
    }

    @Test
    public void monoSubscriberConsumer(){
        String name = "William Suane";
        Mono<String> mono = Mono.just(name).log();

        mono.subscribe(s -> log.info("Value: {}", s));

        log.info("----------------------------------");

        StepVerifier.create(mono).expectNext(name).verifyComplete();

        log.info("Mono {}",mono);
        log.info("Everything working as intended");
    }



    @Test
    public void monoSubscriberConsumerError(){
        String name = "William Suane";
        Mono<String> mono = Mono.just(name).map(s -> {
            throw new RuntimeException("Testing Mono with error");
        });

        mono.subscribe(s -> log.info("Name: {}", s), s-> log.error("Something bad happend"));
        mono.subscribe(s -> log.info("Name: {}", s), Throwable::printStackTrace);

        log.info("----------------------------------");

        StepVerifier.create(mono).expectError(RuntimeException.class).verify();

    }


    @Test
    public void monoSubscriberConsumerComplete(){
        String name = "William Suane";
        Mono<String> mono = Mono.just(name).log().map(String::toUpperCase);

        mono.subscribe(s -> log.info("Value: {}", s), Throwable::printStackTrace, ()->log.info("FINISHED!"));

        log.info("----------------------------------");

        StepVerifier.create(mono).expectNext(name.toUpperCase()).verifyComplete();
    }


    @Test
    public void monoSubscriberConsumerSubscription(){
        String name = "William Suane";
        Mono<String> mono = Mono.just(name).log().map(String::toUpperCase);

        mono.subscribe(s -> log.info("Value: {}", s),
                Throwable::printStackTrace,
                ()->log.info("FINISHED!"),
                subscription -> subscription.request(5)
                );

        StepVerifier.create(mono).expectNext(name.toUpperCase()).verifyComplete();

    }

    @Test
    public void monoDoOnMethods(){
        String name = "William Suane";
        Mono<Object> mono = Mono.just(name)
                .log()
                .map(String::toUpperCase)
                .doOnSubscribe(subscription -> log.info("Subscribed"))
                .doOnRequest(longNumber -> log.info("Request received, starting doing something..."))
                .doOnNext(s -> log.info("Value is here. Executing doOnNext {}", s))
                .flatMap(s -> Mono.empty())
                .doOnNext(s -> log.info("Value is here. Executing doOnNext {}", s)) //this will not be executed
                .doOnSuccess(s -> log.info("doOnSuccess executed {}", s));


        mono.subscribe(s -> log.info("Value: {}", s),
                Throwable::printStackTrace,
                ()->log.info("FINISHED!"),
                subscription -> subscription.request(5)
        );
    }

    @Test
    public void monoDoOnError() {

        String name = "William Suane";
        Mono<Object> error = Mono.error(new IllegalArgumentException("Illeagal argument exception"))
                .onErrorReturn("EMPTY")
                .onErrorResume(s-> {
                    log.info("Inside on error resume");
                    return Mono.just(name);
                })
                .doOnError(
                        e-> MonoTest.log.error("Error message: {}", e.getMessage())).log();


        StepVerifier.create(error).expectNext("EMPTY").verifyComplete();
    }


    @Test
    public void monoDoOnErrorResume() {

        String name = "William Suane";
        Mono<Object> error = Mono.error(new IllegalArgumentException("Illeagal argument exception"))
                .onErrorResume(s-> {
                    log.info("Inside on error resume");
                    return Mono.just(name);
                })
                .onErrorReturn("EMPTY")
                .doOnError(
                        e-> MonoTest.log.error("Error message: {}", e.getMessage())).log();


        StepVerifier.create(error).expectNext(name).verifyComplete();
    }

}
