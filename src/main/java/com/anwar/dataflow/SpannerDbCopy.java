package com.anwar.dataflow;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public class SpannerDbCopy {

	private static final String PROJECT_ID = "doorapi-200221";
	private static final String INSTANCE_DB = "doorapi-200221";
	private static final String TABLE_ID = "doorapi-200221";
	private static final String GCS_STORAGE = "gs://doorapi-200221/";

	public static void main(String[] args) {

		PipelineOptions options = PipelineOptionsFactory.create();

/*		DataflowPipelineOptions dataflowOptions = options.as(DataflowPipelineOptions.class);
		dataflowOptions.setRunner(DataflowRunner.class);
		dataflowOptions.setProject(PROJECT_ID);*/
		// dataflowOptions.setTempLocation("gs://SET_YOUR_BUCKET_NAME_HERE/AND_TEMP_DIRECTORY");
		Pipeline p = Pipeline.create(options);

		//p.apply(SpannerIO.read().withInstanceId(INSTANCE_DB).withDatabaseId(TABLE_ID).withQuery("SELECT id, name FROM users"));

		//PCollection<KV<String, Long>> words = 
				
/*				p.apply(JdbcIO.<KV<String, Long>> read()
				.withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create("nl.topicus.jdbc.CloudSpannerDriver",
						"jdbc:cloudspanner://localhost;Project=my-project-id;Instance=instance-id;Database=database;PvtKeyPath=C:\\Users\\MyUserName\\Documents\\CloudSpannerKeys\\cloudspanner-key.json"))
				.withQuery("SELECT t.table_name FROM information_schema.tables AS t")
				.withCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianLongCoder.of()))
				.withRowMapper(new JdbcIO.RowMapper<KV<String, Long>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public KV<String, Long> mapRow(ResultSet resultSet) throws Exception {
						return KV.of(resultSet.getString(1), resultSet.getLong(2));
					}
				}));*/
	}

}
