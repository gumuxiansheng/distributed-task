drop table if exists dev_distributed_task.task;
create table dev_distributed_task.task(
    task_id serial primary key,
    task_name varchar(50),
    last_start_time timestamp,
    last_end_time timestamp,
    last_host varchar(20),
    next_scheduled_time timestamp
);