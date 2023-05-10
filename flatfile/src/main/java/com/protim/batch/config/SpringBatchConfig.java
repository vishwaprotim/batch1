package com.protim.batch.config;

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.DefaultBatchConfigurer;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.file.FlatFileHeaderCallback;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import com.protim.batch.entity.Record;

@Configuration
@EnableBatchProcessing
public class SpringBatchConfig extends DefaultBatchConfigurer {

    private static final Logger LOGGER = LoggerFactory.getLogger(SpringBatchConfig.class);

    private static final String OUTPUT_NAME = "flatfile/output/output_" + System.currentTimeMillis() + ".csv";
    private static final int THREAD_COUNT = 10;

    private static final int CHUNK = 200;
    private static final boolean THREAD_ENABLED = true;

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Override
    public void setDataSource(DataSource dataSource) {
    } // This BatchConfigurer ignores any DataSource

    @Bean
    public FlatFileItemReader<Record> reader() {
        FlatFileItemReader<Record> itemReader = new FlatFileItemReader<>();
        itemReader.setResource(new FileSystemResource("flatfile/src/main/resources/input.csv"));
        itemReader.setName("csvFileReader");
        itemReader.setLinesToSkip(1);
        itemReader.setLineMapper(new DefaultLineMapper<>() {
            {
                setLineTokenizer(new DelimitedLineTokenizer() {
                    {
                        setNames("id", "name", "subject", "marks", "spec");
                        setStrict(false);
                        setDelimiter(DELIMITER_COMMA);
                    }
                });
                setFieldSetMapper(new BeanWrapperFieldSetMapper<Record>() {
                    {
                        setTargetType(Record.class);
                    }
                });
            }
        });
        return itemReader;
    }

    @Bean
    public RecordProcessor processor() {
        return new RecordProcessor();
    }

    @Bean
    public FlatFileItemWriter<Record> writer() {
        FlatFileItemWriter<Record> writer = new FlatFileItemWriter<>() {
            {
                setName("csvWriter");
                setResource(new FileSystemResource(OUTPUT_NAME));
                setAppendAllowed(true);
                setLineAggregator(new DelimitedLineAggregator<>() {
                    {
                        setDelimiter(",");
                        setFieldExtractor(new BeanWrapperFieldExtractor<>() {
                            {
                                setNames(new String[] { "id", "name", "subject", "grade", "marks", "spec" });
                            }
                        });
                    }
                });

                // Write Header for output CSV File
                setHeaderCallback(new FlatFileHeaderCallback() {
                    @Override
                    public void writeHeader(Writer writer) throws IOException {
                        writer.write("id,name,subject,grade,marks,spec");
                    }
                });
            }

            // Thread safe write method
            @Override
            public synchronized void write(List<? extends Record> items) throws Exception {
                List<Integer> ids = new ArrayList<>();
                items.stream().forEach(i -> ids.add(i.getId()));
                LOGGER.info("Thread " + Thread.currentThread().getName() + ": csvWriter writing " + items.size()
                        + " lines" + " with IDs: " + ids);
                super.write(items);
            }
        };
        return writer;
    }

    @Bean
    public Step step1ProcessCsv() {
        String stepName = "process-csv";
        return (THREAD_ENABLED) ?

                stepBuilderFactory.get(stepName).<Record, Record>chunk(CHUNK)
                        .reader(reader())
                        .processor(processor())
                        .writer(writer())
                        .taskExecutor(taskExecutor())
                        .throttleLimit(THREAD_COUNT) // default is 4
                        .build()

                : stepBuilderFactory.get(stepName).<Record, Record>chunk(CHUNK)
                        .reader(reader())
                        .processor(processor())
                        .writer(writer())
                        .build();

        /*
         * As tested on Core i3 7th gen
         * With threads - job completed in 6s 931 ms
         * Without threads - job completed in 10s 548 ms
         */
    }

    @Bean
    public Job runJob() {
        return jobBuilderFactory.get("csv-job")
                .flow(step1ProcessCsv())
                .end()
                .build();
    }

    @Bean("PoolTaskExecutor")
    public TaskExecutor taskExecutor() {

        // using this instead of SimpleAsyncTaskExecutor for more control
        ThreadPoolTaskExecutor threadPoolTaskExecutor = new ThreadPoolTaskExecutor() {
            @Override
            public Thread createThread(Runnable runnable) {
                Thread thread = super.createThread(runnable);
                LOGGER.info(thread.getName() + " created.");
                return thread;
            }

            {
                setCorePoolSize(THREAD_COUNT); // min #threads remain active at any point, default is 1
                setMaxPoolSize(10); // max threads that can be created depending on queue capcacity
                initialize();
            }
        };

        return threadPoolTaskExecutor;
    }

}
