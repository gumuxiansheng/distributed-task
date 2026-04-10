package cn.mikezhu.distributedtask.task;

import cn.mikezhu.distributedtask.annotation.DistributedTaskAspect;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.Rollback;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * 单元测试：分布式任务执行控制逻辑验证。
 *
 * 测试流程：
 * 1. 准备数据（priorTestRun）
 * 2. 调用任务方法（tasks.testTaskX）
 * 3. 校验任务状态（checkTestRun）
 *
 * 该测试使用事务回滚保证每个用例独立。
 */
@SpringBootTest
@Profile("test")
@Transactional
@Rollback
class TasksTest {
    /**
     * 查询任务状态的 SQL 模板，基于 task_name 和 task_ex 唯一定位一条任务记录。
     */
    public static final String FETCH_SQL = """
            SELECT * FROM dev_distributed_task.task
            WHERE task_name='%s' AND task_ex = '%s'
            LIMIT 1;
            """;

    @Autowired
    private Tasks tasks;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    /**
     * 预置测试场景，设置指定 taskName 对应的任务为可执行（在 SQL 中修改时间/状态）。
     *
     * @param taskName 需要设置状态的任务名称
     */
    private void priorTestRun(String taskName) {
        jdbcTemplate.execute(String.format(DistributedTaskAspect.UPDATE_TASK_SQL,
                "2019-12-31 23:59:59",
                "2019-12-31 23:59:59",
                "2019-12-31 23:59:59",
                "",
                taskName));
    }

    /**
     * 校验指定任务是否在数据库中存在并状态正确。
     *
     * @param taskName 验证的任务名称和值（task_ex）
     */
    private void checkTestRun(String taskName) {
        List<Map<String, Object>> queryList = jdbcTemplate.queryForList(String.format(FETCH_SQL, taskName, taskName));
        assertFalse(queryList.isEmpty());
        assertEquals(taskName, queryList.get(0).get("task_name"));
        assertEquals(taskName, queryList.get(0).get("task_ex"));
    }

    @Test
    void testTask1() {
        // Set up the database
        priorTestRun("testTask1");
        // Run the task
        tasks.testTask1();

        // Task1 executed, Task2 should not be executed
        tasks.testTask2();

        // Verify the result
        checkTestRun("testTask1");
    }

    @Test
    void testTask2() {
        // Set up the database
        priorTestRun("testTask2");
        // Run the task
        tasks.testTask2();

        // Verify the result
        checkTestRun("testTask2");
    }

    @Test
    void testTask3() {
        // Set up the database
        priorTestRun("testTask3");
        // Run the task
        tasks.testTask3();

        // Verify the result
        checkTestRun("testTask3");
    }

    @Test
    void testTask4() {
        // Set up the database
        priorTestRun("testTask4");
        // Run the task
        tasks.testTask4();

        // Verify the result
        checkTestRun("testTask4");
    }

    @Test
    void testTask5() {
        // Set up the database
        priorTestRun("testTask5");
        // Run the task
        tasks.testTask5();

        // Verify the result
        checkTestRun("testTask5");
    }

    @Test
    void testTask6() {
        // Set up the database
        priorTestRun("testTask6");
        // Run the task
        tasks.testTask6();

        // Verify the result
        checkTestRun("testTask6");
    }

    @Test
    void testTask7() {
        // Set up the database
        priorTestRun("testTask7");
        // Run the task
        tasks.testTask7();

        // Verify the result
        checkTestRun("testTask7");
    }

    @Test
    void testTask8() {
        // Set up the database
        priorTestRun("testTask8");
        // Run the task
        tasks.testTask8();

        // Verify the result
        checkTestRun("testTask8");
    }
}
