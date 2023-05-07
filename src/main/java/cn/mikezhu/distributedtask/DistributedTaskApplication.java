package cn.mikezhu.distributedtask;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class DistributedTaskApplication {

    public static void main(String[] args) {
        SpringApplication.run(DistributedTaskApplication.class, args);
    }

}
