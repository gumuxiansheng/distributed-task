package cn.mikezhu.distributedtask.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.support.CronExpression;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

@Component
@Async
public class Tasks {
    private static final Logger logger = LoggerFactory.getLogger(Tasks.class);
    public static final String DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
    public static final String FETCH_LOCK_SQL = """
            SELECT * FROM dev_distributed_task.task
            WHERE task_name='test1' AND next_scheduled_time IS NULL OR next_scheduled_time <= '%s'
            LIMIT 1 FOR UPDATE SKIP LOCKED;
            """;
    public static final String UPDATE_TASK_SQL = """
            UPDATE dev_distributed_task.task
            SET last_start_time='%s',
                 last_end_time='%s',
                 next_scheduled_time='%s',
                 task_ex='%s'
            WHERE task_name='test1';
            """;

    private final JdbcTemplate jdbcTemplate;

    public Tasks(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Scheduled(cron = "2,12,22,32,42,52 * * * * ? ")
    @Transactional
    public void testTask1() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(System.currentTimeMillis());
        String currentTime = new SimpleDateFormat(DATETIME_FORMAT).format(calendar.getTime());
        logger.debug("testTask1 XXXXXXXXX {}, {}", currentTime, Thread.currentThread().getName());
        List<Map<String, Object>> queryList = jdbcTemplate.queryForList(String.format(FETCH_LOCK_SQL, currentTime));
        if (queryList.isEmpty()) {

            return;
        }

        String nextScheduledTime = getNextScheduledTime();
        jdbcTemplate.execute(String.format(UPDATE_TASK_SQL, currentTime, currentTime, nextScheduledTime, "testTask1"));
        logger.info("testTask1 executed at {}", currentTime);
    }

    @Scheduled(cron = "2,12,22,32,42,52 * * * * ? ")
    @Transactional
    public void testTask2() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(System.currentTimeMillis());
        String currentTime = new SimpleDateFormat(DATETIME_FORMAT).format(calendar.getTime());
        logger.debug("testTask2 XXXXXXXXX {}, {}", currentTime, Thread.currentThread().getName());
        List<Map<String, Object>> queryList = jdbcTemplate.queryForList(String.format(FETCH_LOCK_SQL, currentTime));
        if (queryList.isEmpty()) {

            return;
        }

        String nextScheduledTime = getNextScheduledTime();
        jdbcTemplate.execute(String.format(UPDATE_TASK_SQL, currentTime, currentTime, nextScheduledTime, "testTask2"));
        logger.info("testTask2 executed at {}", currentTime);
    }

    @Scheduled(cron = "2,12,22,32,42,52 * * * * ? ")
    @Transactional
    public void testTask3() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(System.currentTimeMillis());
        String currentTime = new SimpleDateFormat(DATETIME_FORMAT).format(calendar.getTime());
        logger.debug("testTask3 XXXXXXXXX {}, {}", currentTime, Thread.currentThread().getName());
        List<Map<String, Object>> queryList = jdbcTemplate.queryForList(String.format(FETCH_LOCK_SQL, currentTime));
        if (queryList.isEmpty()) {

            return;
        }

        String nextScheduledTime = getNextScheduledTime();
        jdbcTemplate.execute(String.format(UPDATE_TASK_SQL, currentTime, currentTime, nextScheduledTime, "testTask3"));
        logger.info("testTask3 executed at {}", currentTime);
    }

    @Scheduled(cron = "2,12,22,32,42,52 * * * * ? ")
    @Transactional
    public void testTask4() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(System.currentTimeMillis());
        String currentTime = new SimpleDateFormat(DATETIME_FORMAT).format(calendar.getTime());
        logger.debug("testTask4 XXXXXXXXX {}, {}", currentTime, Thread.currentThread().getName());
        List<Map<String, Object>> queryList = jdbcTemplate.queryForList(String.format(FETCH_LOCK_SQL, currentTime));
        if (queryList.isEmpty()) {

            return;
        }

        String nextScheduledTime = getNextScheduledTime();
        jdbcTemplate.execute(String.format(UPDATE_TASK_SQL, currentTime, currentTime, nextScheduledTime, "testTask4"));
        logger.info("testTask4 executed at {}", currentTime);
    }

    @Scheduled(cron = "2,12,22,32,42,52 * * * * ? ")
    @Transactional
    public void testTask5() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(System.currentTimeMillis());
        String currentTime = new SimpleDateFormat(DATETIME_FORMAT).format(calendar.getTime());
        logger.debug("testTask5 XXXXXXXXX {}, {}", currentTime, Thread.currentThread().getName());
        List<Map<String, Object>> queryList = jdbcTemplate.queryForList(String.format(FETCH_LOCK_SQL, currentTime));
        if (queryList.isEmpty()) {

            return;
        }

        String nextScheduledTime = getNextScheduledTime();
        jdbcTemplate.execute(String.format(UPDATE_TASK_SQL, currentTime, currentTime, nextScheduledTime, "testTask5"));
        logger.info("testTask5 executed at {}", currentTime);
    }

    @Scheduled(cron = "2,12,22,32,42,52 * * * * ? ")
    @Transactional
    public void testTask6() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(System.currentTimeMillis());
        String currentTime = new SimpleDateFormat(DATETIME_FORMAT).format(calendar.getTime());
        logger.debug("testTask6 XXXXXXXXX {}, {}", currentTime, Thread.currentThread().getName());
        List<Map<String, Object>> queryList = jdbcTemplate.queryForList(String.format(FETCH_LOCK_SQL, currentTime));
        if (queryList.isEmpty()) {

            return;
        }

        String nextScheduledTime = getNextScheduledTime();
        jdbcTemplate.execute(String.format(UPDATE_TASK_SQL, currentTime, currentTime, nextScheduledTime, "testTask6"));
        logger.info("testTask6 executed at {}", currentTime);
    }

    @Scheduled(cron = "2,12,22,32,42,52 * * * * ? ")
    @Transactional
    public void testTask7() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(System.currentTimeMillis());
        String currentTime = new SimpleDateFormat(DATETIME_FORMAT).format(calendar.getTime());
        logger.debug("testTask7 XXXXXXXXX {}, {}", currentTime, Thread.currentThread().getName());
        List<Map<String, Object>> queryList = jdbcTemplate.queryForList(String.format(FETCH_LOCK_SQL, currentTime));
        if (queryList.isEmpty()) {

            return;
        }

        String nextScheduledTime = getNextScheduledTime();
        jdbcTemplate.execute(String.format(UPDATE_TASK_SQL, currentTime, currentTime, nextScheduledTime, "testTask7"));
        logger.info("testTask7 executed at {}", currentTime);
    }

    @Scheduled(cron = "2,12,22,32,42,52 * * * * ? ")
    @Transactional
    public void testTask8() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(System.currentTimeMillis());
        String currentTime = new SimpleDateFormat(DATETIME_FORMAT).format(calendar.getTime());
        logger.debug("testTask8 XXXXXXXXX {}, {}", currentTime, Thread.currentThread().getName());
        List<Map<String, Object>> queryList = jdbcTemplate.queryForList(String.format(FETCH_LOCK_SQL, currentTime));
        if (queryList.isEmpty()) {

            return;
        }

        String nextScheduledTime = getNextScheduledTime();
        jdbcTemplate.execute(String.format(UPDATE_TASK_SQL, currentTime, currentTime, nextScheduledTime, "testTask8"));
        logger.info("testTask8 executed at {}", currentTime);
    }

    private String getNextScheduledTime() {
        CronExpression expression = CronExpression.parse("2,12,22,32,42,52 * * * * ? ");
        LocalDateTime nextTime = expression.next(LocalDateTime.now());
        if (nextTime == null) {
            return "2099-12-31 23:59:59";
        }

        return nextTime.format(DateTimeFormatter.ofPattern(DATETIME_FORMAT));
    }
}
