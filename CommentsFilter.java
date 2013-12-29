package question3;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;

/**
 * Filter out comments by users reputation
 * @author cindyzhang
 *
 */
public class CommentsFilter {
	
	public static class CommentsMapper extends Mapper<LongWritable, Text, Text, Text>{
		Configuration config = HBaseConfiguration.create();
		HBaseAdmin admin;
		HTableDescriptor htd ;
		HTable table;
		private BloomFilter filter = new BloomFilter();
		Text outputKey = new Text();
		Text outputValue = new Text();
		String userId = null;

		/**
		 * Get bloom filter from local file
		 */
		@Override
		protected void setup(Context context) throws IOException {
			Configuration conf = context.getConfiguration();
			Path[] files = DistributedCache.getLocalCacheFiles(conf);
			admin = new HBaseAdmin(config);
			htd = new HTableDescriptor("test");
			table = new HTable(config, htd.getName());

			if (files != null && files.length == 1) {
				System.out.println("Reading Bloom Filter from: "
						+ files[0]);
				// Open local file to read
				DataInputStream strm = new DataInputStream(
						new FileInputStream(files[0].toString()));
				filter.readFields(strm);
				strm.close();
			}
		}
		
		/**
		 * Filter records by bloom filter and combine user information from HBase
		 */
		@Override
		public void map(LongWritable key, Text value, Context context){
			Map<String, String> parsed = MyUtility.transformXmlToMap(value
					.toString());
			if (parsed.containsKey("UserId")) {
				userId = parsed.get("UserId");
				if (filter.membershipTest(new Key(userId.getBytes()))) {	
					outputKey.set("CommentId:"+userId);					
					try {
						Get g = new Get(Bytes.toBytes(userId));
						Result result = table.get(g);
						outputValue.set
						(new String(result
								.getValue(Bytes.toBytes("data"), Bytes.toBytes("1")), "UTF-8"));
						context.write(outputKey, outputValue);
					} catch (Exception e) {
						e.printStackTrace();
					}
					
				}
			}
		}
		
	}

	/**
	 * @param args
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Path inputPath = new Path(args[0]);
		Path cachePath = new Path(args[1]);
		Path outputPath = new Path(args[2]);
//		Path inputPath = new Path("/Users/cindyzhang/InputFile/userpost/comments.xml");
//		Path cachePath = new Path("/Users/cindyzhang/Desktop/filter");
//		Path outputPath = new Path("/Users/cindyzhang/Desktop/output");
		
		Job job = new Job(conf, "CommentsFilter");
		job.setJarByClass(CommentsFilter.class);
		job.setMapperClass(CommentsMapper.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		TextInputFormat.addInputPath(job, inputPath);
		TextOutputFormat.setOutputPath(job, outputPath);

		DistributedCache.addCacheFile(
				FileSystem.get(conf).makeQualified
				(cachePath).toUri(), job.getConfiguration());
		System.exit(job.waitForCompletion(true)? 0 : 1);

	}

}
