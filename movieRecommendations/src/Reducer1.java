import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer1 extends Reducer<Text, Text, Text, Text> {

    private static Text data = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context)
		 					throws IOException, InterruptedException {

            String moviesRatings = "";
            for(Text value : values )
            {
                String [] movieRating = value.toString().split(",");

                moviesRatings = moviesRatings+movieRating[0]+","+movieRating[1]+" ";

            }

            data.set(":"+moviesRatings);
            context.write(key, data);
    }
}
