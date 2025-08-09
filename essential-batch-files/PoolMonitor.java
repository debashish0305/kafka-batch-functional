package com.example.batch;

import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariPoolMXBean;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class PoolMonitor {

    @Autowired
    private DataSource dataSource;

    @Scheduled(fixedDelay = 30000)
    public void logHikariStats() {
        if (dataSource instanceof HikariDataSource) {
            HikariDataSource hikariDataSource = (HikariDataSource) dataSource;
            HikariPoolMXBean poolProxy = hikariDataSource.getHikariPoolMXBean();

            log.info("HikariCP Stats: active={}, idle={}, total={}, waiting={}",
                    poolProxy.getActiveConnections(),
                    poolProxy.getIdleConnections(),
                    poolProxy.getTotalConnections(),
                    poolProxy.getThreadsAwaitingConnection());
        }
    }
}