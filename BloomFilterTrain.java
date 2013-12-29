package question3;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;
import java.util.Set;
import java.util.HashSet;

/**
 * Train a bloom filter
 * @author cindyzhang
 *
 */
public class BloomFilterTrain {
	private static Set<String> addedKeys = new HashSet<String>();
	private static BloomFilter bloomFilter = null;
	private static Key bloomFilterKey = new Key();

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		Path inputFile = new Path(args[0]);// Training data
		int reputation = Integer.parseInt(args[1]);// Reputation number
		Path bfFile = new Path(args[2]);// Cache file location
		
//		Path inputFile = new Path("/Users/cindyzhang/InputFile/userpost/users.xml");//Training data
//		int reputation = 1500;//Reputation number
//		Path bfFile = new Path("/Users/cindyzhang/Desktop/filter");//Cache file location

		int vectorSize = getOptimalBloomFilterSize(150000, 0.01f);
		int nbHash = getOptimalK(150000, vectorSize);

		BloomFilter filter = new BloomFilter(vectorSize, nbHash,
				Hash.MURMUR_HASH);
		System.out.println("Training Bloom filter of size " + vectorSize
				+ " with " + nbHash + " hash functions.");

		String line = null;
		int numElements = 0;
		FileSystem fs = FileSystem.get(new Configuration());

		for (FileStatus status : fs.listStatus(inputFile)) {
			BufferedReader rdr = new BufferedReader(new InputStreamReader(
					fs.open(status.getPath())));
			System.out.println("Reading" + status.getPath());
			while ((line = rdr.readLine()) != null) {
				Map<String, String> parsed = MyUtility.transformXmlToMap(line);
				if (parsed.containsKey("Reputation")) {
					String userId = parsed.get("Id");
					int repuStr = Integer.parseInt(parsed.get("Reputation"));
					if (repuStr >= reputation) {
						filter.add(new Key(userId.getBytes()));
						//System.out.println("Added key " + userId);
						//addedKeys.add(userId);
						numElements++;
					}
				}
			}
			rdr.close();
		}
		System.out.println("Trained Bloom filter with " + numElements
				+ " entries.");
		System.out.println("Serializing Bloom filter to HDFS at " + bfFile);
		FSDataOutputStream strm = fs.create(bfFile);
		filter.write(strm);
		strm.flush();// This forces any buffered output bytes to be written out
						// to the stream.
		strm.close();
//		bloomFilter = filter;
//		testBloomFilter();
	}

	/**
	 * Gets the optimal Bloom filter sized based on the input parameters and the
	 * optimal number of hash functions.
	 * 
	 * @param numElements
	 *            The number of elements used to train the set.
	 * @param falsePosRate
	 *            The desired false positive rate.
	 * @return The optimal Bloom filter size.
	 */
	public static int getOptimalBloomFilterSize(int numElements,
			float falsePosRate) {
		return (int) (-numElements * (float) Math.log(falsePosRate) / Math.pow(
				Math.log(2), 2));
	}

	/**
	 * Gets the optimal-k value based on the input parameters.
	 * 
	 * @param numElements
	 *            The number of elements used to train the set.
	 * @param vectorSize
	 *            The size of the Bloom filter.
	 * @return The optimal-k value, rounded to the closest integer.
	 */
	public static int getOptimalK(float numElements, float vectorSize) {
		return (int) Math.round(vectorSize * Math.log(2) / numElements);
	}

	
	private static void testBloomFilter() {

		System.out.println("Testing Bloomfilter");
		System.out.println("Testing keys present ");
		for (String uid : addedKeys) {
			bloomFilterKey.set(uid.getBytes(), 1.0);
			if (bloomFilter.membershipTest(bloomFilterKey)) {
				System.out.println(uid + " read successfully");
			} else {
				System.out.println(uid
						+ ", not found, FALSE NEGATIVE! BAD BLOOM FILTER");
			}
		}

		for (int i = 13000; i < 13100; i++) {
			String key = String.valueOf(i);
			bloomFilterKey.set(key.getBytes(), 1.0);
			if (bloomFilter.membershipTest(bloomFilterKey)
					&& !addedKeys.contains(key)) {
				System.out.println("False postive " + key);
			}
		}
	}
	

}
