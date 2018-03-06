import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Set;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Mapper1 extends Mapper<Object, Text, Text, Text> {

	private static Text movieIDs = new Text();
  private static Text ratings = new Text();

	public void map(Object key, Text input, Context context) throws IOException, InterruptedException {

    	  String [] lines = input.toString().split("\\s+");

				if(lines.length >= 2)
				{
					 movieIDs.set(lines[0]);
					 ratings.set(lines[1].trim());
					 context.write(movieIDs, ratings);
				}
	 }
}
