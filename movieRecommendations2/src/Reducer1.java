import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer1 extends Reducer<Text, Text, Text, Text> {

    private static Text movieIDs = new Text();
    private static Text ratings = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context)
		 					throws IOException, InterruptedException {

            for(Text value : values )
            {
                String [] movieRatings = value.toString().split(" ");

                if(movieRatings.length >=2)
                {
                   for(int i = 0; i<movieRatings.length; i++)
                   {
                      for(int k = i+1; k < movieRatings.length; k++)
                      {

                        String combinations = movieRatings[i]+ ","+movieRatings[k];
                        String [] movieAndRating = combinations.split(",");
                        String movieCombinations = movieAndRating[0] + "," + movieAndRating[2];
                        String ratingCombinations = movieAndRating[1] + "," +movieAndRating[3];
                        movieIDs.set(movieCombinations);
                        ratings.set(ratingCombinations);
                        context.write(movieIDs, ratings);
                      }
                   }
                }
            }
    }
}
