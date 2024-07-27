package cn.mikezhu.distributedtask.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Component
@Async
public class Tasks {
    private static final Logger logger = LoggerFactory.getLogger(Tasks.class);

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Scheduled(cron = "0/10 * * * * ?")
    @Transactional
    public void testTask1() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(System.currentTimeMillis());
        String currentTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(calendar.getTime());
        logger.info("testTask1 XXXXXXXXX {}, {}", currentTime, Thread.currentThread().getName());
        List<Map<String, Object>> queryList = jdbcTemplate.queryForList(String.format("""
                SELECT * FROM dev_distributed_task.task
                WHERE task_name='test1' AND next_scheduled_time IS NULL OR next_scheduled_time <= '%s'
                LIMIT 1 FOR UPDATE SKIP LOCKED;
                """, currentTime));
        if (queryList.isEmpty()) {
            logger.info("map is empty");
            return;
        }

        calendar.add(Calendar.SECOND, 10);
        String nextScheduledTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(calendar.getTime());
        jdbcTemplate.execute(String.format("""
                UPDATE dev_distributed_task.task
                SET last_start_time='%s',
                     last_end_time='%s',
                     next_scheduled_time='%s',
                     task_ex='%s'
                WHERE task_name='test1';
                """, currentTime, currentTime, nextScheduledTime, "testTask1"));
        logger.info("testTask1 executed at {}", currentTime);
    }

    @Scheduled(cron = "0/10 * * * * ?")
    @Transactional
    public void testTask2() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(System.currentTimeMillis());
        String currentTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(calendar.getTime());
        logger.info("testTask2 XXXXXXXXX {}, {}", currentTime, Thread.currentThread().getName());
        List<Map<String, Object>> queryList = jdbcTemplate.queryForList(String.format("""
                SELECT * FROM dev_distributed_task.task
                WHERE task_name='test1' AND next_scheduled_time IS NULL OR next_scheduled_time <= '%s'
                LIMIT 1 FOR UPDATE SKIP LOCKED;
                """, currentTime));
        if (queryList.isEmpty()) {
            logger.info("map is empty");
            return;
        }

        calendar.add(Calendar.SECOND, 10);
        String nextScheduledTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(calendar.getTime());
        jdbcTemplate.execute(String.format("""
                UPDATE dev_distributed_task.task
                SET last_start_time='%s',
                     last_end_time='%s',
                     next_scheduled_time='%s',
                     task_ex='%s'
                WHERE task_name='test1';
                """, currentTime, currentTime, nextScheduledTime, "testTask2"));
        logger.info("testTask2 executed at {}", currentTime);
    }

    @Scheduled(cron = "0/10 * * * * ?")
    @Transactional
    public void testTask3() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(System.currentTimeMillis());
        String currentTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(calendar.getTime());
        logger.info("testTask3 XXXXXXXXX {}, {}", currentTime, Thread.currentThread().getName());
        List<Map<String, Object>> queryList = jdbcTemplate.queryForList(String.format("""
                SELECT * FROM dev_distributed_task.task
                WHERE task_name='test1' AND next_scheduled_time IS NULL OR next_scheduled_time <= '%s'
                LIMIT 1 FOR UPDATE SKIP LOCKED;
                """, currentTime));
        if (queryList.isEmpty()) {
            logger.info("map is empty");
            return;
        }

        calendar.add(Calendar.SECOND, 10);
        String nextScheduledTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(calendar.getTime());
        jdbcTemplate.execute(String.format("""
                UPDATE dev_distributed_task.task
                SET last_start_time='%s',
                     last_end_time='%s',
                     next_scheduled_time='%s',
                     task_ex='%s'
                WHERE task_name='test1';
                """, currentTime, currentTime, nextScheduledTime, "testTask3"));
        logger.info("testTask3 executed at {}", currentTime);
    }

    @Scheduled(cron = "0/10 * * * * ?")
    @Transactional
    public void testTask4() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(System.currentTimeMillis());
        String currentTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(calendar.getTime());
        logger.info("testTask4 XXXXXXXXX {}, {}", currentTime, Thread.currentThread().getName());
        List<Map<String, Object>> queryList = jdbcTemplate.queryForList(String.format("""
                SELECT * FROM dev_distributed_task.task
                WHERE task_name='test1' AND next_scheduled_time IS NULL OR next_scheduled_time <= '%s'
                LIMIT 1 FOR UPDATE SKIP LOCKED;
                """, currentTime));
        if (queryList.isEmpty()) {
            logger.info("map is empty");
            return;
        }

        calendar.add(Calendar.SECOND, 10);
        String nextScheduledTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(calendar.getTime());
        jdbcTemplate.execute(String.format("""
                UPDATE dev_distributed_task.task
                SET last_start_time='%s',
                     last_end_time='%s',
                     next_scheduled_time='%s',
                     task_ex='%s'
                WHERE task_name='test1';
                """, currentTime, currentTime, nextScheduledTime, "testTask4"));
        logger.info("testTask4 executed at {}", currentTime);
    }

    @Scheduled(cron = "0/10 * * * * ?")
    @Transactional
    public void testTask5() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(System.currentTimeMillis());
        String currentTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(calendar.getTime());
        logger.info("testTask5 XXXXXXXXX {}, {}", currentTime, Thread.currentThread().getName());
        List<Map<String, Object>> queryList = jdbcTemplate.queryForList(String.format("""
                SELECT * FROM dev_distributed_task.task
                WHERE task_name='test1' AND next_scheduled_time IS NULL OR next_scheduled_time <= '%s'
                LIMIT 1 FOR UPDATE SKIP LOCKED;
                """, currentTime));
        if (queryList.isEmpty()) {
            logger.info("map is empty");
            return;
        }

        calendar.add(Calendar.SECOND, 10);
        String nextScheduledTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(calendar.getTime());
        jdbcTemplate.execute(String.format("""
                UPDATE dev_distributed_task.task
                SET last_start_time='%s',
                     last_end_time='%s',
                     next_scheduled_time='%s',
                     task_ex='%s'
                WHERE task_name='test1';
                """, currentTime, currentTime, nextScheduledTime, "testTask5"));
        logger.info("testTask5 executed at {}", currentTime);
    }

    @Scheduled(cron = "0/10 * * * * ?")
    @Transactional
    public void testTask6() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(System.currentTimeMillis());
        String currentTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(calendar.getTime());
        logger.info("testTask6 XXXXXXXXX {}, {}", currentTime, Thread.currentThread().getName());
        List<Map<String, Object>> queryList = jdbcTemplate.queryForList(String.format("""
                SELECT * FROM dev_distributed_task.task
                WHERE task_name='test1' AND next_scheduled_time IS NULL OR next_scheduled_time <= '%s'
                LIMIT 1 FOR UPDATE SKIP LOCKED;
                """, currentTime));
        if (queryList.isEmpty()) {
            logger.info("map is empty");
            return;
        }

        calendar.add(Calendar.SECOND, 10);
        String nextScheduledTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(calendar.getTime());
        jdbcTemplate.execute(String.format("""
                UPDATE dev_distributed_task.task
                SET last_start_time='%s',
                     last_end_time='%s',
                     next_scheduled_time='%s',
                     task_ex='%s'
                WHERE task_name='test1';
                """, currentTime, currentTime, nextScheduledTime, "testTask6"));
        logger.info("testTask6 executed at {}", currentTime);
    }

    @Scheduled(cron = "0/10 * * * * ?")
    @Transactional
    public void testTask7() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(System.currentTimeMillis());
        String currentTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(calendar.getTime());
        logger.info("testTask7 XXXXXXXXX {}, {}", currentTime, Thread.currentThread().getName());
        List<Map<String, Object>> queryList = jdbcTemplate.queryForList(String.format("""
                SELECT * FROM dev_distributed_task.task
                WHERE task_name='test1' AND next_scheduled_time IS NULL OR next_scheduled_time <= '%s'
                LIMIT 1 FOR UPDATE SKIP LOCKED;
                """, currentTime));
        if (queryList.isEmpty()) {
            logger.info("map is empty");
            return;
        }

        calendar.add(Calendar.SECOND, 10);
        String nextScheduledTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(calendar.getTime());
        jdbcTemplate.execute(String.format("""
                UPDATE dev_distributed_task.task
                SET last_start_time='%s',
                     last_end_time='%s',
                     next_scheduled_time='%s',
                     task_ex='%s'
                WHERE task_name='test1';
                """, currentTime, currentTime, nextScheduledTime, "testTask7"));
        logger.info("testTask7 executed at {}", currentTime);
    }

    @Scheduled(cron = "0/10 * * * * ?")
    @Transactional
    public void testTask8() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(System.currentTimeMillis());
        String currentTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(calendar.getTime());
        logger.info("testTask8 XXXXXXXXX {}, {}", currentTime, Thread.currentThread().getName());
        List<Map<String, Object>> queryList = jdbcTemplate.queryForList(String.format("""
                SELECT * FROM dev_distributed_task.task
                WHERE task_name='test1' AND next_scheduled_time IS NULL OR next_scheduled_time <= '%s'
                LIMIT 1 FOR UPDATE SKIP LOCKED;
                """, currentTime));
        if (queryList.isEmpty()) {
            logger.info("map is empty");
            return;
        }

        calendar.add(Calendar.SECOND, 10);
        String nextScheduledTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(calendar.getTime());
        jdbcTemplate.execute(String.format("""
                UPDATE dev_distributed_task.task
                SET last_start_time='%s',
                     last_end_time='%s',
                     next_scheduled_time='%s',
                     task_ex='%s'
                WHERE task_name='test1';
                """, currentTime, currentTime, nextScheduledTime, "testTask8"));
        logger.info("testTask8 executed at {}", currentTime);
    }
}
