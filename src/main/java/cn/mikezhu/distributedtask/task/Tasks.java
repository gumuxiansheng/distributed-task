package cn.mikezhu.distributedtask.task;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class Tasks {
    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Scheduled(cron = "0/10 * * * * ?")
    public void testTask1() {

    }
}
