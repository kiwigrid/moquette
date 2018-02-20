package io.moquette.server;

import java.util.concurrent.ScheduledExecutorService;

import com.hazelcast.core.HazelcastInstance;
import io.moquette.spi.impl.ProtocolProcessor;

public interface IServer {
    ProtocolProcessor getProcessor();

    ScheduledExecutorService getScheduler();

    HazelcastInstance getHazelcastInstance();
}
