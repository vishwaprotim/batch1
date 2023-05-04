package com.protim.batch.config;

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

import com.protim.batch.entity.Record;

@Configuration
@EnableBatchProcessing
public class SpringBatchConfig extends DefaultBatchConfigurer {

    private static final Logger LOGGER = LoggerFactory.getLogger(SpringBatchConfig.class);
    private static final int CHUNK = 100;

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
            @Override
            public String doWrite(List<? extends Record> items) {
                LOGGER.info("csvWriter invoked for writing #lines: " + items.size());
                return super.doWrite(items);
            }
        };
        writer.setName("csvWriter");
        writer.setResource(new FileSystemResource("flatfile/output/output.csv"));
        writer.setAppendAllowed(true);
        writer.setLineAggregator(new DelimitedLineAggregator<>() {
            {
                setDelimiter(",");
                setFieldExtractor(new BeanWrapperFieldExtractor<>() {
                    {
                        setNames(new String[] { "id", "name", "subject", "grade", "marks", "spec" });
                    }
                });
            }
        });
        return writer;
    }

    @Bean
    public Step step1() {
        return stepBuilderFactory.get("process-csv").<Record, Record>chunk(CHUNK)
                .reader(reader())
                .processor(processor())
                .writer(writer()).build();
    }

    @Bean
    public Job runJob() {
        return jobBuilderFactory.get("csv-job")
                .flow(step1()).end().build();
    }

}
