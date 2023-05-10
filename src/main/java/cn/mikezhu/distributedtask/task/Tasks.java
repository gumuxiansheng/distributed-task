package cn.mikezhu.distributedtask.task;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.text.SimpleDateFormat;
import java.util.Calendar;

@Component
public class Tasks {
    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Scheduled(cron = "0/10 * * * * ?")
    public void testTask1() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(System.currentTimeMillis());
        jdbcTemplate.execute(String.format("""
                UPDATE dev_distributed_task.task
                SET last_start_time='%s'
                WHERE task_name='test1';
                """, new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(calendar.getTime())));
    }
}
