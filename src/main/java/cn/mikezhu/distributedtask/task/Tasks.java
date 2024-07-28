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

    private void startRun(String taskName) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(System.currentTimeMillis());
        String currentTime = new SimpleDateFormat(DATETIME_FORMAT).format(calendar.getTime());
        logger.debug("{} XXXXXXXXX {}, {}", taskName, currentTime, Thread.currentThread().getName());
        List<Map<String, Object>> queryList = jdbcTemplate.queryForList(String.format(FETCH_LOCK_SQL, currentTime));
        if (queryList.isEmpty()) {

            return;
        }

        String nextScheduledTime = getNextScheduledTime();
        jdbcTemplate.execute(String.format(UPDATE_TASK_SQL, currentTime, currentTime, nextScheduledTime, "testTask1"));
        logger.info("{} executed at {}", taskName, currentTime);
    }

    @Scheduled(cron = "2,12,22,32,42,52 * * * * ? ")
    @Transactional
    public void testTask1() {
        startRun("testTask1");
    }

    @Scheduled(cron = "2,12,22,32,42,52 * * * * ? ")
    @Transactional
    public void testTask2() {
        startRun("testTask2");
    }

    @Scheduled(cron = "2,12,22,32,42,52 * * * * ? ")
    @Transactional
    public void testTask3() {
        startRun("testTask3");
    }

    @Scheduled(cron = "2,12,22,32,42,52 * * * * ? ")
    @Transactional
    public void testTask4() {
        startRun("testTask4");
    }

    @Scheduled(cron = "2,12,22,32,42,52 * * * * ? ")
    @Transactional
    public void testTask5() {
        startRun("testTask5");
    }

    @Scheduled(cron = "2,12,22,32,42,52 * * * * ? ")
    @Transactional
    public void testTask6() {
        startRun("testTask6");
    }

    @Scheduled(cron = "2,12,22,32,42,52 * * * * ? ")
    @Transactional
    public void testTask7() {
        startRun("testTask7");
    }

    @Scheduled(cron = "2,12,22,32,42,52 * * * * ? ")
    @Transactional
    public void testTask8() {
        startRun("testTask8");
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
