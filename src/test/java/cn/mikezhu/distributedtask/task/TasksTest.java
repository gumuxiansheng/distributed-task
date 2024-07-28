package cn.mikezhu.distributedtask.task;

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

@SpringBootTest
@Profile("test")
@Transactional
@Rollback
class TasksTest {
    public static final String FETCH_SQL = """
            SELECT * FROM dev_distributed_task.task
            WHERE task_name='test1' AND task_ex = '%s'
            LIMIT 1;
            """;

    @Autowired
    private Tasks tasks;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private void priorTestRun() {
        jdbcTemplate.execute(String.format(Tasks.UPDATE_TASK_SQL, "2019-12-31 23:59:59", "2019-12-31 23:59:59", "2019-12-31 23:59:59", ""));
    }
    private void checkTestRun(String funcName) {
        List<Map<String, Object>> queryList = jdbcTemplate.queryForList(String.format(FETCH_SQL, funcName));
        assertFalse(queryList.isEmpty());
        assertEquals("test1", queryList.get(0).get("task_name"));
        assertEquals(funcName, queryList.get(0).get("task_ex"));

    }

    @Test
    void testTask1() {
        // Set up the database
        priorTestRun();
        // Run the task
        tasks.testTask1();

        // Verify the result
        checkTestRun("testTask1");
    }

    @Test
    void testTask2() {
        // Set up the database
        priorTestRun();
        // Run the task
        tasks.testTask2();

        // Verify the result
        checkTestRun("testTask2");
    }

    @Test
    void testTask3() {
        // Set up the database
        priorTestRun();
        // Run the task
        tasks.testTask3();

        // Verify the result
        checkTestRun("testTask3");
    }

    @Test
    void testTask4() {
        // Set up the database
        priorTestRun();
        // Run the task
        tasks.testTask4();

        // Verify the result
        checkTestRun("testTask4");
    }

    @Test
    void testTask5() {
        // Set up the database
        priorTestRun();
        // Run the task
        tasks.testTask5();

        // Verify the result
        checkTestRun("testTask5");
    }

    @Test
    void testTask6() {
        // Set up the database
        priorTestRun();
        // Run the task
        tasks.testTask6();

        // Verify the result
        checkTestRun("testTask6");
    }

    @Test
    void testTask7() {
        // Set up the database
        priorTestRun();
        // Run the task
        tasks.testTask7();

        // Verify the result
        checkTestRun("testTask7");
    }

    @Test
    void testTask8() {
        // Set up the database
        priorTestRun();
        // Run the task
        tasks.testTask8();

        // Verify the result
        checkTestRun("testTask8");
    }
}
