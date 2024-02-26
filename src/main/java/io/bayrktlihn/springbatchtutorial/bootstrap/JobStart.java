package io.bayrktlihn.springbatchtutorial.bootstrap;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.HashMap;

@Component
@RequiredArgsConstructor
public class JobStart implements CommandLineRunner {

    private final Job csvReaderDatabaseWriterJob;
    private final Job jsonReaderXmlWriterJob;
    private final Job xmlReaderJsonWriterJob;
    private final Job csvReaderJpaWriterJob;
    private final JobLauncher jobLauncher;


    @Override
    public void run(String... args) throws Exception {
        HashMap<String, JobParameter<?>> parameters = new HashMap<>();
        parameters.put("currentTime", new JobParameter<>(Instant.now().toEpochMilli(), Long.class));

        JobParameters jobParameters = new JobParameters(parameters);
        jobLauncher.run(csvReaderJpaWriterJob, jobParameters);
    }
}
