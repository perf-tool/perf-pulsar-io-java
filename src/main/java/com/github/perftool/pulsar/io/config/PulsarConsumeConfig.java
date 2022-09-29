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

package com.github.perftool.pulsar.io.config;

import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Configuration
@Service
public class PulsarConsumeConfig {

    @Value("${PULSAR_CONSUME_TENANT:public}")
    public String tenant;

    @Value("${PULSAR_CONSUME_NAMESPACE:default}")
    public String namespace;

    @Value("${PULSAR_CONSUME_TOPIC:consume-topic}")
    public String topic;

    @Value("${PULSAR_SUBSCRIPTION_TYPE:Failover}")
    public SubscriptionType subscriptionType;

    @Value("${PULSAR_SUBSCRIPTION_NAME:}")
    public String subscriptionName;

    @Value("${PULSAR_SUBSCRIPTION_INITIAL_POSITION:Latest}")
    public SubscriptionInitialPosition subscriptionInitialPosition;

    @Value("${PULSAR_CONSUME_RATE_LIMITER:-1}")
    public int rateLimiter;

    public String getSubscriptionName() {
        if (StringUtils.isEmpty(subscriptionName)) {
            return UUID.randomUUID().toString();
        }
        return subscriptionName;
    }

}
