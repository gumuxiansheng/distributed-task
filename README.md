# Distributed Task
A preemptive distributed task scheduling system, developed based on Spring Boot, implementing the preemptive distributed task scheduling function. No scheduling center is used, and task preemption is achieved through database locks.

抢占式分布式任务调度系统，基于Spring Boot开发，实现抢占式分布式任务调度功能。无调度中心，通过数据库锁实现任务抢占。

## Project Introduction
This project is a distributed task scheduling system developed based on Spring Boot, implementing the preemptive distributed task scheduling function. The system uses a design without a scheduling center and achieves task preemption through database locks to ensure that tasks can be correctly executed in a distributed environment. It is suitable for extending a single-machine system to a distributed system to achieve efficient task scheduling and execution.

本项目是一个基于Spring Boot开发的分布式任务调度系统，实现了抢占式分布式任务调度功能。系统采用无调度中心的设计，通过数据库锁实现任务抢占，确保任务在分布式环境下能够正确执行。
适合由单机系统扩展到分布式系统，实现任务的高效调度和执行。