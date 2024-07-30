package cn.mikezhu.distributedtask.annotation;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
@Aspect
public class DistributedTaskAspect {

    private static final Logger LOGGER = LoggerFactory.getLogger(DistributedTaskAspect.class);

    @Around(value = "@annotation(DistributedTask)")
    public Object aroundMethod(ProceedingJoinPoint jp) throws Throwable {
        LOGGER.info("annotation DistributedTask");
        return jp.proceed();
    }
}
