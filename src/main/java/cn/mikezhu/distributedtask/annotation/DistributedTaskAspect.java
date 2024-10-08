package cn.mikezhu.distributedtask.annotation;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.support.CronExpression;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import java.lang.reflect.Method;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

@Component
@Aspect
public class DistributedTaskAspect {

    private static final Logger logger = LoggerFactory.getLogger(DistributedTaskAspect.class);

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

    private final TransactionTemplate transactionTemplate;

    public DistributedTaskAspect(JdbcTemplate jdbcTemplate, TransactionTemplate transactionTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        this.transactionTemplate = transactionTemplate;
    }

    @Around(value = "@annotation(DistributedTask)")
    public Object aroundMethod(ProceedingJoinPoint jp) {
        logger.info("annotation DistributedTask");
        MethodSignature signature = (MethodSignature) jp.getSignature();
        Method method = signature.getMethod();
        DistributedTask distributedTaskAnno = method.getAnnotation(DistributedTask.class);
        String taskName = distributedTaskAnno.name();
        String cron = distributedTaskAnno.cron();
        Boolean goRun = transactionTemplate.execute(tx -> {
            try {
                return requireToken(taskName, cron);
            } catch (Exception e) {
                logger.error("DistributedTaskAspect error", e);
            }
            return false;
        });

        if (Boolean.TRUE.equals(goRun)) {
            try {
                logger.info("{} executed", taskName);
                return jp.proceed();
            } catch (Throwable e) {
                logger.error(e.getMessage());
            }
        }

        return null;
    }

    private boolean requireToken(String taskName, String cron) {

        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(System.currentTimeMillis());
        String currentTime = new SimpleDateFormat(DATETIME_FORMAT).format(calendar.getTime());
        logger.info("{} query for token {}", taskName, currentTime);
        List<Map<String, Object>> queryList = jdbcTemplate.queryForList(String.format(FETCH_LOCK_SQL, currentTime));
        if (queryList.isEmpty()) {
            return false;
        }

        String nextScheduledTime = getNextScheduledTime(cron);
        jdbcTemplate.execute(String.format(UPDATE_TASK_SQL, currentTime, currentTime, nextScheduledTime, taskName));

        return true;
    }

    private String getNextScheduledTime(String cron) {
        CronExpression expression = CronExpression.parse(cron);
        LocalDateTime nextTime = expression.next(LocalDateTime.now());
        if (nextTime == null) {
            return "2099-12-31 23:59:59";
        }

        return nextTime.format(DateTimeFormatter.ofPattern(DATETIME_FORMAT));
    }
}
