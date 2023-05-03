package com.protim.batch.config;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.DefaultBatchConfigurer;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.FieldExtractor;
import org.springframework.batch.item.file.transform.LineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;

import com.protim.batch.entity.Record;

@Configuration
@EnableBatchProcessing
public class SpringBatchConfig extends DefaultBatchConfigurer {

    @Override
    public void setDataSource(DataSource dataSource) {
        // This BatchConfigurer ignores any DataSource
    }

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Bean
    public FlatFileItemReader<Record> reader() {
        FlatFileItemReader<Record> itemReader = new FlatFileItemReader<>();
        itemReader.setResource(new FileSystemResource("flatfile/src/main/resources/input.csv"));
        itemReader.setName("csvFileReader");
        itemReader.setLinesToSkip(1);
        itemReader.setLineMapper(lineMapper());

        return itemReader;

    }

    @Bean
    public RecordProcessor processor() {
        return new RecordProcessor();
    }

    @Bean
    public FlatFileItemWriter<Record> writer() {
        FlatFileItemWriter<Record> writer = new FlatFileItemWriter<>();
        writer.setResource(new FileSystemResource("flatfile/output/output.csv"));
        writer.setAppendAllowed(true);
        writer.setLineAggregator(aggregator());
        return writer;
    }

    @Bean
    public Step step1() {
        return stepBuilderFactory.get("process-csv").<Record, Record>chunk(10)
                .reader(reader())
                .processor(processor())
                .writer(writer()).build();
    }

    @Bean
    public Job runJob() {
        return jobBuilderFactory.get("csv-job")
                .flow(step1()).end().build();
    }

    private LineAggregator<Record> aggregator() {
        DelimitedLineAggregator<Record> aggregator = new DelimitedLineAggregator<>();
        aggregator.setDelimiter(",");
        aggregator.setFieldExtractor(fieldExtractor());
        return aggregator;
    }

    private FieldExtractor<Record> fieldExtractor() {
        BeanWrapperFieldExtractor<Record> extractor = new BeanWrapperFieldExtractor<>();
        extractor.setNames(new String[] { "id", "name", "subject", "grade", "marks", "spec" });
        return extractor;
    }

    private LineMapper<Record> lineMapper() {
        DelimitedLineTokenizer delimitedLineTokenizer = new DelimitedLineTokenizer(",");
        delimitedLineTokenizer.setStrict(false);
        delimitedLineTokenizer.setNames("id", "name", "subject", "marks", "spec");

        BeanWrapperFieldSetMapper<Record> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(Record.class);

        DefaultLineMapper<Record> lineMapper = new DefaultLineMapper<>();
        lineMapper.setLineTokenizer(delimitedLineTokenizer);
        lineMapper.setFieldSetMapper(fieldSetMapper);
        return lineMapper;
    }
}
