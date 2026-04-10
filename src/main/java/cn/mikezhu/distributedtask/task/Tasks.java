package cn.mikezhu.distributedtask.task;

import cn.mikezhu.distributedtask.annotation.DistributedTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * 分布式任务执行组件
 * 使用Spring框架的异步任务和分布式任务注解实现
 */
@Component
@Async
public class Tasks {
    // 日志记录器，用于记录任务执行信息
    private static final Logger logger = LoggerFactory.getLogger(Tasks.class);
    // 日期时间格式常量，用于格式化任务执行时间
    public static final String DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";

    /**
     * 任务执行方法
     * @param taskName 任务名称，用于标识当前执行的任务
     */
    private void startRun(String taskName) {
        // 获取当前时间并按照指定格式格式化
        String currentTime = LocalDateTime.now().format(DateTimeFormatter.ofPattern(DATETIME_FORMAT));
        // 记录任务开始执行的信息
        logger.info("{} start run {}", taskName, currentTime);
    }

    /**
     * 分布式任务1
     * 配置了cron表达式，每分钟的2、12、22、32、42、52秒执行
     */
    @DistributedTask(name = "testTask1", cron = "2,12,22,32,42,52 * * * * ?")
    public void testTask1() {
        startRun("testTask1");
    }

    /**
     * 分布式任务2
     * 配置了cron表达式，每分钟的2、12、22、32、42、52秒执行
     */
    @DistributedTask(name = "testTask2", cron = "2,12,22,32,42,52 * * * * ?")
    public void testTask2() {
        startRun("testTask2");
    }

    /**
     * 分布式任务3
     * 配置了cron表达式，每分钟的2、12、22、32、42、52秒执行
     */
    @DistributedTask(name = "testTask3", cron = "2,12,22,32,42,52 * * * * ?")
    public void testTask3() {
        startRun("testTask3");
    }

    /**
     * 分布式任务4
     * 配置了cron表达式，每分钟的2、12、22、32、42、52秒执行
     */
    @DistributedTask(name = "testTask4", cron = "2,12,22,32,42,52 * * * * ?")
    public void testTask4() {
        startRun("testTask4");
    }

    /**
     * 分布式任务5
     * 配置了cron表达式，每分钟的2、12、22、32、42、52秒执行
     */
    @DistributedTask(name = "testTask5", cron = "2,12,22,32,42,52 * * * * ?")
    public void testTask5() {
        startRun("testTask5");
    }

    /**
     * 分布式任务6
     * 配置了cron表达式，每分钟的2、12、22、32、42、52秒执行
     */
    @DistributedTask(name = "testTask6", cron = "2,12,22,32,42,52 * * * * ?")
    public void testTask6() {
        startRun("testTask6");
    }

    /**
     * 分布式任务7
     * 配置了cron表达式，每分钟的2、12、22、32、42、52秒执行
     */
    @DistributedTask(name = "testTask7", cron = "2,12,22,32,42,52 * * * * ?")
    public void testTask7() {
        startRun("testTask7");
    }

    /**
     * 分布式任务8
     * 配置了cron表达式，每分钟的2、12、22、32、42、52秒执行
     */
    @DistributedTask(name = "testTask8", cron = "2,12,22,32,42,52 * * * * ?")
    public void testTask8() {
        startRun("testTask8");
    }

}
