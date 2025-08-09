package com.example.batch;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.support.SqlPagingQueryProviderFactoryBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.jdbc.core.BeanPropertyRowMapper;

@Configuration
@EnableBatchProcessing
@Slf4j
public class BatchConfig {

    private static final int PAGE_SIZE = 1000;
    private static final int GRID_SIZE = 10;

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    private DataSource dataSource;

    @Autowired
    private DbConcurrencyLimiter concurrencyLimiter;

    // Virtual Thread TaskExecutor
    @Bean
    public TaskExecutor taskExecutor() {
        Executor vThreads = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor();
        return task -> vThreads.execute(task);
    }

    // Partitioner - divides the ID range into partitions
    @Bean
    public Partitioner rangePartitioner() {
        return gridSize -> {
            Map<String, ExecutionContext> partitions = new HashMap<>();

            int totalRecords = 100_000; // replace with dynamic count if possible
            int chunkSize = totalRecords / GRID_SIZE;

            int fromId = 1;
            int toId = chunkSize;

            for (int i = 0; i < GRID_SIZE; i++) {
                ExecutionContext context = new ExecutionContext();
                context.putInt("fromId", fromId);
                context.putInt("toId", toId);
                context.putString("partitionName", "partition" + i);
                partitions.put("partition" + i, context);

                fromId = toId + 1;
                toId += chunkSize;
            }

            return partitions;
        };
    }

    // Slave step to process each partition chunk-wise
    @Bean
    public Step slaveStep() {
        return stepBuilderFactory.get("slaveStep")
            .<Record, Record>chunk(PAGE_SIZE)
            .reader(partitionedItemReader(null, null))
            .processor(itemProcessor())
            .writer(partitionedItemWriter(null))
            .build();
    }

    // Master step with partitioner and virtual thread task executor
    @Bean
    public Step masterStep() {
        return stepBuilderFactory.get("masterStep")
            .partitioner(slaveStep().getName(), rangePartitioner())
            .step(slaveStep())
            .gridSize(GRID_SIZE)
            .taskExecutor(taskExecutor())
            .build();
    }

    // Job
    @Bean
    public Job exportJob() {
        return jobBuilderFactory.get("exportJob")
            .start(masterStep())
            .build();
    }

    // Reader, reads records in the assigned id range, with DB concurrency limiter
    @Bean
    @StepScope
    public ItemReader<Record> partitionedItemReader(
            @Value("#{stepExecutionContext['fromId']}") Integer fromId,
            @Value("#{stepExecutionContext['toId']}") Integer toId) throws Exception {

        concurrencyLimiter.acquire();

        JdbcPagingItemReader<Record> reader = new JdbcPagingItemReader<>();
        reader.setDataSource(dataSource);
        reader.setFetchSize(PAGE_SIZE);
        reader.setRowMapper(new BeanPropertyRowMapper<>(Record.class));

        SqlPagingQueryProviderFactoryBean queryProvider = new SqlPagingQueryProviderFactoryBean();
        queryProvider.setDataSource(dataSource);
        queryProvider.setSelectClause("SELECT id, name, value"); // Adjust columns
        queryProvider.setFromClause("FROM my_table");
        queryProvider.setWhereClause("WHERE id >= " + fromId + " AND id <= " + toId);
        queryProvider.setSortKey("id");
        reader.setQueryProvider(queryProvider.getObject());

        reader.afterPropertiesSet();

        concurrencyLimiter.release();

        return reader;
    }

    // Processor (pass-through or implement logic)
    @Bean
    public ItemProcessor<Record, Record> itemProcessor() {
        return item -> {
            // Add processing logic here if needed
            return item;
        };
    }

    // Writer writes to a partition-specific temp file
    @Bean
    @StepScope
    public ItemWriter<Record> partitionedItemWriter(
            @Value("#{stepExecutionContext['partitionName']}") String partitionName) {
        return items -> {
            Path tempFile = Path.of(System.getProperty("java.io.tmpdir"), "export-" + partitionName + ".jsonl");

            try (BufferedWriter writer = Files.newBufferedWriter(tempFile,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
                for (Record record : items) {
                    String jsonLine = String.format("{"id":%d,"name":"%s","value":"%s"}",
                            record.getId(), record.getName(), record.getValue());
                    writer.write(jsonLine);
                    writer.newLine();
                }
            } catch (IOException e) {
                log.error("Error writing partition file: " + tempFile, e);
                throw e;
            }
        };
    }
}