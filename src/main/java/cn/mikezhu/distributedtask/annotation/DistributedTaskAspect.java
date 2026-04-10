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

/**
 * 分布式任务切面类，用于处理分布式任务的调度和执行
 * 通过AOP实现分布式锁，确保同一时间只有一个节点执行任务
 */
@Component
@Aspect
public class DistributedTaskAspect {

    private static final Logger logger = LoggerFactory.getLogger(DistributedTaskAspect.class);

    // 日期时间格式常量
    public static final String DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
    // 获取任务锁的SQL语句，使用FOR UPDATE SKIP LOCKED实现乐观锁
    public static final String FETCH_LOCK_SQL = """
            SELECT * FROM dev_distributed_task.task
            WHERE task_name='%s' AND (next_scheduled_time IS NULL OR next_scheduled_time <= '%s')
            LIMIT 1 FOR UPDATE SKIP LOCKED;
            """;
    // 更新任务状态的SQL语句，记录任务的执行时间和下次执行时间
    public static final String UPDATE_TASK_SQL = """
            UPDATE dev_distributed_task.task
            SET last_start_time='%s',
                 last_end_time='%s',
                 next_scheduled_time='%s',
                 task_ex='%s'
            WHERE task_name='%s';
            """;

    private final JdbcTemplate jdbcTemplate;    // JDBC模板，用于数据库操作

    private final TransactionTemplate transactionTemplate;  // 事务模板，确保操作在事务中执行

    /**
     * 构造函数，注入必要的依赖
     * @param jdbcTemplate JDBC模板
     * @param transactionTemplate 事务模板
     */
    public DistributedTaskAspect(JdbcTemplate jdbcTemplate, TransactionTemplate transactionTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        this.transactionTemplate = transactionTemplate;
    }

    /**
     * 环绕通知，处理带有@DistributedTask注解的方法
     * @param jp 连接点，可以获取方法信息
     * @return 方法执行结果，如果未获取到锁则返回null
     */
    @Around(value = "@annotation(DistributedTask)")
    public Object aroundMethod(ProceedingJoinPoint jp) {
        logger.info("annotation DistributedTask");
        MethodSignature signature = (MethodSignature) jp.getSignature();
        Method method = signature.getMethod();
        DistributedTask distributedTaskAnno = method.getAnnotation(DistributedTask.class);
        String taskName = distributedTaskAnno.name();
        String cron = distributedTaskAnno.cron();
        // 在事务中尝试获取任务执行权
        Boolean goRun = transactionTemplate.execute(tx -> {
            try {
                return requireToken(taskName, cron);
            } catch (Exception e) {
                logger.error("DistributedTaskAspect error", e);
            }
            return false;
        });

        // 如果获取到锁，则执行任务
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
        List<Map<String, Object>> queryList = jdbcTemplate.queryForList(String.format(FETCH_LOCK_SQL, taskName, currentTime));
        if (queryList.isEmpty()) {
            return false;
        }

        String nextScheduledTime = getNextScheduledTime(cron);
        jdbcTemplate.execute(String.format(UPDATE_TASK_SQL, currentTime, currentTime, nextScheduledTime, taskName, taskName));

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
