/*
 * Copyright 2012-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.minihu.springboot.redis.slave.configuration;

import com.minihu.springboot.redis.slave.JedisSentinelSlaveConnectionFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;

@Configuration
@ConditionalOnClass(RedisOperations.class)
@EnableConfigurationProperties(RedisProperties.class)
@Import(JedisSentinelSlaveConnectionConfiguration.class)
public class JedisSentinelSlaveAutoConfiguration {


    @Bean(name = "readerRedisTemplate")
    @ConditionalOnMissingBean(name = "readerRedisTemplate")
    public RedisTemplate<Object, Object> readerRedisTemplate(
            JedisSentinelSlaveConnectionFactory jedisSentinelSlaveConnectionFactory) {
        RedisTemplate<Object, Object> template = new RedisTemplate<>();
        template.setConnectionFactory(jedisSentinelSlaveConnectionFactory);
        return template;
    }

    @Bean(name = "readerStringRedisTemplate")
    @ConditionalOnMissingBean(name = "readerStringRedisTemplate")
    public StringRedisTemplate readerStringRedisTemplate(
            JedisSentinelSlaveConnectionFactory jedisSentinelSlaveConnectionFactory) {
        //ReaderStringRedisTemplate template = new ReaderStringRedisTemplate();
        //template.setConnectionFactory(jedisSentinelSlaveConnectionFactory);
        StringRedisTemplate stringRedisTemplate = new StringRedisTemplate();
        stringRedisTemplate.setConnectionFactory(jedisSentinelSlaveConnectionFactory);
        return stringRedisTemplate;
    }

}
