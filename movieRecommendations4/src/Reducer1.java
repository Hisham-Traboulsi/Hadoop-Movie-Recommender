import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.text.DecimalFormat;

public class Reducer1 extends Reducer<Text, Text, Text, Text> {

    private static Text movieIDs = new Text();
    private static Text ratings = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context)
		 					throws IOException, InterruptedException {

          for(Text value : values)
          {
              String [] movieIDsAndTotalRatings = key.toString().split("\\s+");
              int n = Integer.parseInt(movieIDsAndTotalRatings[1]);
              String [] ratingPairs = value.toString().split("(,|\\s+)");

              double correlationCoefficient = calculateCC(n, ratingPairs);

              if(!Double.isNaN(correlationCoefficient))
              {
                  movieIDs.set(movieIDsAndTotalRatings[0]);
                  ratings.set(Double.toString(correlationCoefficient));
                  context.write(movieIDs, ratings);
              }
          }
    }

    public static double calculateCC(int n, String [] ratings)
    {
        double [] xpoints = new double[n];
        double [] ypoints = new double[n];

        int currentCountX = 0;
        int currentCountY = 0;
        for(int i = 0; i<ratings.length; i++)
        {
           if(i % 2 == 0)
           {
              xpoints[currentCountX] = Double.parseDouble(ratings[i]);
              currentCountX++;
           }
           else
           {
              ypoints[currentCountY] = Double.parseDouble(ratings[i]);
              currentCountY++;
           }
        }

        double meanOfX = 0;
        double meanOfY = 0;

        for(int i = 0; i<xpoints.length; i++)
        {
          meanOfX += xpoints[i];
          meanOfY += ypoints[i];
        }

        meanOfX = meanOfX/xpoints.length;
        meanOfY = meanOfY/ypoints.length;

        // System.out.println("The mean of X is: "+meanOfX);
        // System.out.println("The mean of Y is: "+meanOfY);

        double sdx = 0;
        double sdy = 0;

        for(int i = 0; i<xpoints.length; i++)
        {
          sdx += Math.pow((xpoints[i] - meanOfX), 2);
          sdy += Math.pow((ypoints[i] - meanOfY), 2);
        }

        sdx = Math.sqrt(sdx/(xpoints.length - 1));
        sdy = Math.sqrt(sdy/(ypoints.length - 1));

        // System.out.println("The sdx is: "+sdx);
        // System.out.println("The sdy is: "+sdy);

        double sdxy =  0;

        for(int i = 0; i<xpoints.length; i++)
        {
          sdxy += ((xpoints[i]-meanOfX)/sdx)*((ypoints[i]-meanOfY)/sdy);
        }

        double r = ((double) 1/ ((double) xpoints.length - 1)) * sdxy;

        if(r < 0)
        {
            r = r*-1;
        }

        if(!Double.isNaN(r))
  		  {
  			     DecimalFormat df = new DecimalFormat("###.###");
  			     r = Double.parseDouble(df.format(r));
        }
        return r;
        //System.out.println(r);
      }
}
