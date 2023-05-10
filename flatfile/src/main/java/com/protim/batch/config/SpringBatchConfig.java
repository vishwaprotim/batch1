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
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;

import com.protim.batch.entity.Record;

@Configuration
@EnableBatchProcessing
public class SpringBatchConfig extends DefaultBatchConfigurer {

    private static final Logger LOGGER = LoggerFactory.getLogger(SpringBatchConfig.class);

    private static final String OUTPUT_NAME = "flatfile/output/output_" + System.currentTimeMillis() + ".csv";
    private static final String THREAD_PREFIX = "csv_batch_thread_";

    private static final int CHUNK = 200;
    private static final int CONCURRENCY = 10;
    private static final boolean THREAD_ENABLED = true;

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Override
    public void setDataSource(DataSource dataSource) {
        // This BatchConfigurer ignores any DataSource
    }

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
                LOGGER.info("csvWriter invoked for writing #lines: " + items.size() + " with IDs: " + ids);
                super.write(items);
            }
        };
        return writer;
    }

    @Bean
    public Step step1() {
        return (THREAD_ENABLED) ?

                stepBuilderFactory.get("process-csv").<Record, Record>chunk(CHUNK)
                        .reader(reader())
                        .processor(processor())
                        .writer(writer())
                        .taskExecutor(asyncTaskExecutor())
                        .build()
                : stepBuilderFactory.get("process-csv").<Record, Record>chunk(CHUNK)
                        .reader(reader())
                        .processor(processor())
                        .writer(writer())
                        .build();

    }

    @Bean
    public Job runJob() {
        return jobBuilderFactory.get("csv-job")
                .flow(step1()).end().build();
    }

    @Bean
    public TaskExecutor asyncTaskExecutor() {
        SimpleAsyncTaskExecutor executor = new SimpleAsyncTaskExecutor(THREAD_PREFIX) {
            @Override
            public Thread createThread(Runnable runnable) {
                Thread thread = super.createThread(runnable);
                LOGGER.info("creating thread: " + thread.getName());
                return thread;
            }

            {
                setConcurrencyLimit(CONCURRENCY);

            }
        };
        return executor;
    }

}
