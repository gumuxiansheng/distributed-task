package cn.mikezhu.distributedtask.task;

import cn.mikezhu.distributedtask.annotation.DistributedTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@Component
@Async
public class Tasks {
    private static final Logger logger = LoggerFactory.getLogger(Tasks.class);
    public static final String DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";

    private void startRun(String taskName) {
        String currentTime = LocalDateTime.now().format(DateTimeFormatter.ofPattern(DATETIME_FORMAT));
        logger.info("{} start run {}", taskName, currentTime);
    }

    @Transactional
    @DistributedTask(name = "testTask1", cron = "2,12,22,32,42,52 * * * * ?")
    public void testTask1() {
        startRun("testTask1");
    }

    @DistributedTask(name = "testTask2", cron = "2,12,22,32,42,52 * * * * ?")
    @Transactional
    public void testTask2() {
        startRun("testTask2");
    }

    @DistributedTask(name = "testTask3", cron = "2,12,22,32,42,52 * * * * ?")
    @Transactional
    public void testTask3() {
        startRun("testTask3");
    }

    @DistributedTask(name = "testTask4", cron = "2,12,22,32,42,52 * * * * ?")
    @Transactional
    public void testTask4() {
        startRun("testTask4");
    }

    @DistributedTask(name = "testTask5", cron = "2,12,22,32,42,52 * * * * ?")
    @Transactional
    public void testTask5() {
        startRun("testTask5");
    }

    @DistributedTask(name = "testTask6", cron = "2,12,22,32,42,52 * * * * ?")
    @Transactional
    public void testTask6() {
        startRun("testTask6");
    }

    @DistributedTask(name = "testTask7", cron = "2,12,22,32,42,52 * * * * ?")
    @Transactional
    public void testTask7() {
        startRun("testTask7");
    }

    @DistributedTask(name = "testTask8", cron = "2,12,22,32,42,52 * * * * ?")
    @Transactional
    public void testTask8() {
        startRun("testTask8");
    }

}
