import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer1 extends Reducer<Text, Text, Text, Text> {

    private static Text ratings = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context)
		 					throws IOException, InterruptedException {


            int sum = 0;
            String ratingPairs = "";
            for(Text value : values )
            {
                sum++;
                ratingPairs += value.toString() + " ";
            }

            ratings.set(sum+":"+ratingPairs);
            context.write(key, ratings);
    }
}
