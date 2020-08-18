import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Map.Entry;

public class PeopleYouMightKnow extends Configured implements Tool{
	
	static public class MutualFriendWritable implements Writable {
        public Long user;
        public Long mutualFriend;
        
        // Default Constructor of the class
        public MutualFriendWritable() {
            this(-1L, -1L);
        }
        
        // Custom Constructor of the class
        public MutualFriendWritable(Long user, Long mutualFriend) {
            this.user = user;
            this.mutualFriend = mutualFriend;
        }
        
        @Override
        public void readFields(DataInput in) throws IOException {
            user = in.readLong();
            mutualFriend = in.readLong();
        }
        
        @Override
        public void write(DataOutput out) throws IOException {
            out.writeLong(user);
            out.writeLong(mutualFriend);
        }    
	}
	
	public static void main(String[] args) throws Exception {
	      System.out.println(Arrays.toString(args));
	      int res = ToolRunner.run(new Configuration(), new PeopleYouMightKnow(), args);
	      
	      System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		Job job = new Job(getConf(), "PeopleYouMightKnow");
		job.setJarByClass(PeopleYouMightKnow.class);
		
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(MutualFriendWritable.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// Read the input and output path from the arguments provided
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
		return 0;
   }

   public static class Map extends Mapper<LongWritable, Text, LongWritable, MutualFriendWritable> {

	  @Override
      public void map(LongWritable key, Text value, Context context)
              throws IOException, InterruptedException {
		  String[] user_friends_list = value.toString().split("\t");
		  Long userID = Long.parseLong(user_friends_list[0]);
		  
		  // If the user has a list of friends, iterate that friend list
		  if (user_friends_list.length > 1) {
			  
			  // Friend list is a comma separated list
			  // Make a string array by using this property
			  String[] friends = user_friends_list[1].toString().split(",");
			  int num_friends = friends.length;

			  for (int i = 0; i < num_friends; i ++) {
				  Long friend = Long.parseLong(friends[i]);
				  
				  // As the user and the friend are already connected, make the mutual friend -1
				  context.write(new LongWritable(userID), new MutualFriendWritable(friend, -1L));
				  
				  // Add the user as a mutual friend for all the possible pairs of his/ her friends
				  for (int j=i+1; j<num_friends; j++) {
					  Long other_friend = Long.parseLong(friends[j]);
					  context.write(new LongWritable(friend), new MutualFriendWritable(other_friend, userID));
					  context.write(new LongWritable(other_friend), new MutualFriendWritable(friend, userID));
				  }  
			  } 
		  }
      }
	}

   	public static class Reduce extends Reducer<LongWritable, MutualFriendWritable, LongWritable, Text> {
	  @Override
      public void reduce(LongWritable key, Iterable<MutualFriendWritable> values, Context context)
              throws IOException, InterruptedException {
		  
		  // Create a hashmap for each key i.e. user
		  final java.util.Map<Long, List<Long>> recommendedFriends = 
				  new HashMap<Long, List<Long>>();
		  
		  for (MutualFriendWritable val: values) {
              final Long userID = val.user;
              final Long mutualFriend = val.mutualFriend;
              
              if (recommendedFriends.containsKey(userID)) {
                  if(val.mutualFriend == -1){
                	  recommendedFriends.put(userID, null);
                  } 
                  else if (recommendedFriends.get(userID) != null){
                	  recommendedFriends.get(userID).add(mutualFriend);
                  }
              } 
              else {
            	  if(val.mutualFriend == -1){
                	  recommendedFriends.put(userID, null);
                  } 
                  else{
                	  recommendedFriends.put(userID, new ArrayList<Long>() {
                		  {add(mutualFriend);}
                	  });
              		}
              }
		  }
		  
		  // Create a priority queue to sort the friend recommendation based on number of mutual friends
		  PriorityQueue<Entry<Long, List<Long>>> sortedRecommendedFriends = 
				  new PriorityQueue<Entry<Long, List<Long>>>( 
						  new Comparator<Entry<Long, List<Long>>>() {
							  @Override
					          public int compare(Entry<Long, List<Long>> o1, Entry<Long, List<Long>> o2) {
								  Integer v1 = o1.getValue().size();
								  Integer v2 = o2.getValue().size();
								  if(v1 > v2){
									  return -1;
								  }
								  // if there is a tie, sort based on numerically ascending order
								  else if (v1.equals(v2) && o1.getKey() < o2.getKey()) {
				                      return -1;
				                  } 
								  else {
				                      return 1;
				                  }
					          }
				  		  });
		  
		  // Add sorted order of the hash map in a new map 
		  for (java.util.Map.Entry<Long, List<Long>> entry : recommendedFriends.entrySet()) {
              if (entry.getValue() != null) {
                  sortedRecommendedFriends.add(entry);
              }
          }
		  
		  StringBuffer output = new StringBuffer();
          int i = 0;
          int size = sortedRecommendedFriends.size();
          // Recommend the top 10 friends
          while (!sortedRecommendedFriends.isEmpty()) {
            output.append(sortedRecommendedFriends.poll().getKey());
            if (i >= 9 || i >= size-1) break;
            i ++;
            output.append(",");
          }
          context.write(key, new Text(output.toString()));
      }
	}
}


