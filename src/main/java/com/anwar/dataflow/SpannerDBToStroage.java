package com.anwar.dataflow;

import java.sql.ResultSet;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

public class SpannerDBToStroage {

	public interface WordCountOptions extends PipelineOptions {

		@Description("Path of the file to read from")
		@Default.String("gs://doorapi-200221/")
		String getInputFile();

		void setInputFile(String value);

		@Description("JDBC Driver to use for writing the data")
		@Default.String("nl.topicus.jdbc.CloudSpannerDriver")
		String getJdbcDriver();

		void setJdbcDriver(String driver);

		@Description("JDBC url to write the data to")
		@Default.String("jdbc:cloudspanner://localhost;Project=doorapi-200221;Instance=test-instance;Database=test;PvtKeyPath=C://Users/Anwar/doorapi.json")
		String getURL();

		void setURL(String url);
	}

	private static final String SELECT_SQL = "SELECT * FROM person";

	public static void main(String[] args) {
		WordCountOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(WordCountOptions.class);
		DataflowPipelineOptions dataflowOptions = options.as(DataflowPipelineOptions.class);
		dataflowOptions.setRunner(DataflowRunner.class);
		dataflowOptions.setProject("doorapi-200221");
		dataflowOptions.setTempLocation("/sample/tempdir");
		dataflowOptions.setTemplateLocation("/sample");

		Pipeline p = Pipeline.create(options);

		p.apply(JdbcIO.<String> read()
				.withDataSourceConfiguration(
						JdbcIO.DataSourceConfiguration.create(options.getJdbcDriver(), options.getURL()))
				.withCoder(StringUtf8Coder.of()).withQuery(SELECT_SQL).withRowMapper(new JdbcIO.RowMapper<String>() {
					@Override
					public String mapRow(ResultSet resultSet) throws Exception {
						int id = resultSet.getInt(1);
						String name = resultSet.getString(2);
						int age = resultSet.getInt(3);
						return id + "##" + name + "##" + age;
					}

				})).apply("WriteStorage", TextIO.write().to("gs://doorapi-200221/person1.txt"));

		p.run().waitUntilFinish();
	}
}
