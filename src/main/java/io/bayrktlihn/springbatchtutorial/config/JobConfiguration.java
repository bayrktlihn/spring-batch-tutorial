package io.bayrktlihn.springbatchtutorial.config;

import io.bayrktlihn.springbatchtutorial.dto.StudentDto;
import io.bayrktlihn.springbatchtutorial.entity.Student;
import jakarta.persistence.EntityManagerFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.SkipListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.annotation.OnSkipInProcess;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.skip.AlwaysSkipItemSkipPolicy;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JpaItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.json.JacksonJsonObjectMarshaller;
import org.springframework.batch.item.json.JacksonJsonObjectReader;
import org.springframework.batch.item.json.builder.JsonFileItemWriterBuilder;
import org.springframework.batch.item.json.builder.JsonItemReaderBuilder;
import org.springframework.batch.item.xml.builder.StaxEventItemReaderBuilder;
import org.springframework.batch.item.xml.builder.StaxEventItemWriterBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.oxm.jaxb.Jaxb2Marshaller;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

@Configuration
@RequiredArgsConstructor
@Slf4j
public class JobConfiguration {

    private final JobRepository jobRepository;
    private final PlatformTransactionManager platformTransactionManager;
    private final DataSource dataSource;
    private final EntityManagerFactory entityManagerFactory;

    @Bean
    Job csvReaderDatabaseWriterJob() {
        return new JobBuilder("csvReaderDatabaseWriterJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(csvReaderDatabaseWriterJobFirstStep())
                .build();
    }

    @Bean
    Job csvReaderJpaWriterJob() {
        return new JobBuilder("csvReaderJpaWriterJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(csvReaderJpaWriterJobFirstStep())
                .build();
    }

    private Step csvReaderJpaWriterJobFirstStep() {
        return new StepBuilder("csvReaderJpaWriterJobFirstStep", jobRepository)
                .<StudentDto, Student>chunk(3, platformTransactionManager)
                .faultTolerant()
                .skip(Exception.class)
                .skipLimit(Integer.MAX_VALUE)
                .reader(csvReaderDatabaseWriterJobFirstStepReader())
                .processor(item -> {
                    Student student = new Student();
                    student.setFirstName(item.getFirstName());
                    student.setLastName(item.getLastName());
                    student.setEmail(item.getEmail());
                    return student;
                })
                .writer(csvReaderJpaWriterJobFirstStepWriter())
                .build();
    }

    private ItemWriter<? super Student> csvReaderJpaWriterJobFirstStepWriter() {
        return new JpaItemWriterBuilder<>()
                .entityManagerFactory(entityManagerFactory)
                .clearPersistenceContext(true)
                .build();
    }

    @Bean
    Job jsonReaderXmlWriterJob() {
        return new JobBuilder("jsonReaderXmlWriterJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(jsonReaderXmlWriterJobFirstStep())
                .build();
    }

    @Bean
    Job xmlReaderJsonWriterJob() {
        return new JobBuilder("xmlReaderJsonWriterJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(xmlReaderJsonWriterJobFirstStep())
                .build();
    }

    private Step xmlReaderJsonWriterJobFirstStep() {
        return new StepBuilder("xmlReaderJsonWriterJobFirstStep", jobRepository)
                .<StudentDto, StudentDto>chunk(3, platformTransactionManager)
                .reader(xmlReaderJsonWriterJobFirstStepReader())
                .writer(xmlReaderJsonWriterJobFirstStepWriter())
                .build();
    }

    private ItemWriter<? super StudentDto> xmlReaderJsonWriterJobFirstStepWriter() {
        JacksonJsonObjectMarshaller<StudentDto> jsonObjectMarshaller = new JacksonJsonObjectMarshaller<>();
        return new JsonFileItemWriterBuilder<StudentDto>()
                .name("xmlReaderJsonWriterJobFirstStepWriter")
                .jsonObjectMarshaller(jsonObjectMarshaller)
                .resource(new FileSystemResource("C:\\Users\\bayrktlihn\\Desktop\\students.json"))
                .build();
    }

    private ItemReader<? extends StudentDto> xmlReaderJsonWriterJobFirstStepReader() {
        Jaxb2Marshaller unmarshaller = new Jaxb2Marshaller();
        unmarshaller.setClassesToBeBound(StudentDto.class);

        return new StaxEventItemReaderBuilder<StudentDto>()
                .name("xmlReaderJsonWriterJobFirstStepReader")
                .resource(new FileSystemResource("C:\\Users\\bayrktlihn\\Desktop\\students.xml"))
                .addFragmentRootElements("student")
                .unmarshaller(unmarshaller)
                .build();

    }

    private Step jsonReaderXmlWriterJobFirstStep() {
        return new StepBuilder("jsonReaderXmlWriterJobFirstStep", jobRepository)
                .<StudentDto, StudentDto>chunk(3, platformTransactionManager)
                .reader(jsonReaderXmlWriterJobFirstStepReader())
                .writer(jsonReaderXmlWriterJobFirstStepWriter())
                .build();
    }

    private ItemWriter<? super StudentDto> jsonReaderXmlWriterJobFirstStepWriter() {
        FileSystemResource fileSystemResource = new FileSystemResource("C:\\Users\\bayrktlihn\\Desktop\\students.xml");

        Jaxb2Marshaller marshaller = new Jaxb2Marshaller();
        marshaller.setClassesToBeBound(StudentDto.class);

        return new StaxEventItemWriterBuilder<>()
                .name("jsonReaderXmlWriterJobFirstStepWriter")
                .resource(fileSystemResource)
                .marshaller(marshaller)
                .rootTagName("students")
                .build();
    }

    private ItemReader<? extends StudentDto> jsonReaderXmlWriterJobFirstStepReader() {


        ClassPathResource classPathResource = new ClassPathResource("reader-files/students.json");

        return new JsonItemReaderBuilder<StudentDto>()
                .name("jsonReaderXmlWriterJobFirstStepReader")
                .jsonObjectReader(new JacksonJsonObjectReader<>(StudentDto.class))
                .resource(classPathResource)
                .build();
    }

    private Step csvReaderDatabaseWriterJobFirstStep() {
        return new StepBuilder("csvReaderDatabaseWriterJobFirstStep", jobRepository)
                .<StudentDto, StudentDto>chunk(3, platformTransactionManager)
                .reader(csvReaderDatabaseWriterJobFirstStepReader())
                .processor(csvReaderDatabaseWriterJobFirstStepProcessor())
                .writer(csvReaderDatabaseWriterJobFirstStepWriter())
                .faultTolerant()
//                .skip(IllegalArgumentException.class)
//                .skipLimit(Integer.MAX_VALUE)
                .skipPolicy(new AlwaysSkipItemSkipPolicy())
                .listener(new SkipListener<>() {
                    @Override
                    public void onSkipInRead(Throwable t) {
                        SkipListener.super.onSkipInRead(t);
                        if (t instanceof FlatFileParseException ffpe) {
                            log.info(ffpe.getInput());
                            log.info(t.toString());
                        }
                    }

                    @Override
                    public void onSkipInProcess(StudentDto item, Throwable t) {
                        SkipListener.super.onSkipInProcess(item, t);
                        if (t instanceof NullPointerException npe) {
                            log.info(t.toString());
                        }
                    }

                    @Override
                    public void onSkipInWrite(StudentDto item, Throwable t) {
                        SkipListener.super.onSkipInWrite(item, t);
                        log.info(t.toString());
                    }
                })
                .build();
    }

    private ItemProcessor<? super StudentDto, ? extends StudentDto> csvReaderDatabaseWriterJobFirstStepProcessor() {
        return (ItemProcessor<StudentDto, StudentDto>) item -> {
            if (item.getId() == 4L) {
                throw new NullPointerException();
            }
            return item;
        };
    }

    @Bean
    public ItemWriter<? super StudentDto> csvReaderDatabaseWriterJobFirstStepWriter() {
        return new JdbcBatchItemWriterBuilder<StudentDto>()
                .dataSource(dataSource)
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
                .sql("insert into student(first_name, last_name, email) values(?, ?, ?)")
                .itemPreparedStatementSetter((item, ps) -> {
                    ps.setString(1, item.getFirstName());
                    ps.setString(2, item.getLastName());
                    ps.setString(3, item.getEmail());
                })
                .build();

    }

    private ItemReader<? extends StudentDto> csvReaderDatabaseWriterJobFirstStepReader() {

        ClassPathResource classPathResource = new ClassPathResource("reader-files/students.csv");


        DefaultLineMapper<StudentDto> lineMapper = new DefaultLineMapper<>();
        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
        lineMapper.setLineTokenizer(tokenizer);


        return new FlatFileItemReaderBuilder<StudentDto>()
                .name("csvReaderDatabaseWriterJobFirstStepReader")
                .resource(classPathResource)
                .linesToSkip(1)
                .delimited()
                .names("ID", "First Name", "Last Name", "Email")
                .targetType(StudentDto.class)
                .build();
    }


}
