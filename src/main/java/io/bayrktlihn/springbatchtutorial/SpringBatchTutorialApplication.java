package io.bayrktlihn.springbatchtutorial;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.util.Map;
import java.util.concurrent.Executor;

@SpringBootApplication
@EnableAsync
@EnableScheduling
@Slf4j
public class SpringBatchTutorialApplication {

    public static void main(String[] args) {
        ConfigurableApplicationContext context = SpringApplication.run(SpringBatchTutorialApplication.class, args);


        showAvailableExecutors(context);
    }

    private static void showAvailableExecutors(ConfigurableApplicationContext context) {
        Map<String, Executor> beans = context.getBeansOfType(Executor.class);

        for (Map.Entry<String, Executor> nameBean : beans.entrySet()) {
            String name = nameBean.getKey();
            Executor bean = nameBean.getValue();

            log.info(name + " " + bean.getClass().getName());
        }
    }


}
