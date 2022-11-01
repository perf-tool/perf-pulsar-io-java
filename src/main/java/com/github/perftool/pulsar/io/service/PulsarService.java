/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.github.perftool.pulsar.io.service;

import com.github.perftool.pulsar.io.config.PulsarConfig;
import com.github.perftool.pulsar.io.config.PulsarConsumeConfig;
import com.github.perftool.pulsar.io.config.PulsarProduceConfig;
import com.google.common.util.concurrent.RateLimiter;
import lombok.extern.log4j.Log4j2;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;

@Log4j2
@Service
public class PulsarService {

    @Autowired
    private PulsarConfig pulsarConfig;

    @Autowired
    private PulsarConsumeConfig consumeConfig;

    @Autowired
    private PulsarProduceConfig produceConfig;

    private PulsarClient pulsarClient;

    private Producer<byte[]> producer;

    private Consumer<byte[]> consumer;

    private RateLimiter rateLimiter;

    @PostConstruct
    public void postConstruct() throws Exception {
        ClientBuilder clientBuilder = PulsarClient.builder().ioThreads(pulsarConfig.ioThreads);
        pulsarClient = clientBuilder.serviceUrl(String.format("http://%s:%s", pulsarConfig.host,
                pulsarConfig.port)).build();
        producer = pulsarClient.newProducer()
                .topic(String.format("persistent://%s/%s/%s",
                        produceConfig.tenant, produceConfig.namespace, produceConfig.topic))
                .create();
        consumer = pulsarClient.newConsumer()
                .topic(String.format("persistent://%s/%s/%s",
                        consumeConfig.tenant, consumeConfig.namespace, consumeConfig.topic))
                .subscriptionName(consumeConfig.getSubscriptionName())
                .subscriptionType(consumeConfig.subscriptionType)
                .subscriptionInitialPosition(consumeConfig.subscriptionInitialPosition)
                .messageListener(this::received)
                .subscribe();
        this.rateLimiter = consumeConfig.rateLimiter == -1 ? null : RateLimiter.create(consumeConfig.rateLimiter);
    }

    public void received(Consumer<byte[]> consumer, Message<byte[]> msg) {
        if (rateLimiter != null) {
            rateLimiter.acquire();
        }
        producer.sendAsync(msg.getData()).whenComplete((messageId, throwable) -> {
            if (throwable != null) {
                log.error("send producer msg error ", throwable);
                return;
            }
            log.info("send success, msg id is {}", messageId);
            consumer.acknowledgeAsync(msg);
        });
    }

}
