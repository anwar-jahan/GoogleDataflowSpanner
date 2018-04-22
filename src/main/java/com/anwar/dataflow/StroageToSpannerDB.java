package com.anwar.dataflow;

import java.sql.PreparedStatement;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

public class StroageToSpannerDB {

	static class WordCountPreparedStatementSetter implements JdbcIO.PreparedStatementSetter<String> {
		private static final long serialVersionUID = 1L;

		public void setParameters(String element, PreparedStatement preparedStatement) throws Exception {
			String[] arr = element.split("##");
			preparedStatement.setInt(1, Integer.parseInt(arr[0]));
			preparedStatement.setString(1, arr[1]);
			preparedStatement.setInt(1, Integer.parseInt(arr[2]));
		}
	}

	public interface WordCountOptions extends PipelineOptions {

		@Description("Path of the file to read from")
		@Default.String("gs://doorapi-200221/person.txt")
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

	private static final String INSERT_OR_UPDATE_SQL = "INSERT INTO person (id, name, age) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE";

	public static void main(String[] args) {
		WordCountOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(WordCountOptions.class);
		DataflowPipelineOptions dataflowOptions = options.as(DataflowPipelineOptions.class);
		dataflowOptions.setRunner(DataflowRunner.class);
		dataflowOptions.setProject("doorapi-200221");
		dataflowOptions.setTempLocation("/sample/tempdir");
		dataflowOptions.setTemplateLocation("/sample");

		Pipeline p = Pipeline.create(options);

		p.apply("ReadLines", TextIO.read().from(options.getInputFile()))
				.apply(JdbcIO.<String> write()
						.withDataSourceConfiguration(
								JdbcIO.DataSourceConfiguration.create(options.getJdbcDriver(), options.getURL()))
						.withStatement(INSERT_OR_UPDATE_SQL)
						.withPreparedStatementSetter(new WordCountPreparedStatementSetter()));

		p.run().waitUntilFinish();
	}
}
