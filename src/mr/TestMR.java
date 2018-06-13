package mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class TestMR {

	public static class Solver implements WritableComparable<Solver> {
		private String solver;
		private String time;

		public Solver() {
		}

		public Solver(String solver, String time) {
			super();
			this.setSolver(solver);
			this.setTime(time);

		}

		@Override
		public void readFields(DataInput input) throws IOException {
			setSolver(WritableUtils.readString(input));
			setTime(WritableUtils.readString(input));
		}

		@Override
		public void write(DataOutput output) throws IOException {
			WritableUtils.writeString(output, getSolver());
			WritableUtils.writeString(output, getTime());
		}

		@Override
		public int compareTo(Solver sol) {
			int compare = getSolver().compareTo(sol.getSolver());
			double i = Double.parseDouble(getTime());
			double j = Double.parseDouble(sol.getTime());
			if (compare == 0) {
				if (i > j) {
					return 1;
				} else if (i < j) {
					return -1;
				} else
					return 0;
			}
			return compare;
		}

		@Override
		public String toString() {
			return String.format("%s %s", solver, time);
		}

		public String getTime() {
			return time;
		}

		public void setTime(String time) {
			this.time = time;
		}

		public String getSolver() {
			return solver;
		}

		public void setSolver(String solver) {
			this.solver = solver;
		}

	}

	public static class Comparator extends WritableComparator {

		protected Comparator() {
			super(Solver.class, true);
		}

		@Override
		public int compare(WritableComparable a, WritableComparable c) {
			Solver ac = (Solver) a;
			Solver bc = (Solver) c;
			return -ac.compareTo(bc);
		}
	}

	public static class TestMap extends Mapper<LongWritable, Text, Solver, Text> {
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Solver, Text>.Context context)
				throws IOException, InterruptedException {
			String[] readRow = value.toString().split("\t+");
			if (readRow[14].contains("solved"))
				context.write(new Solver(readRow[0], readRow[11]), new Text(""));

		}
	}

	public static class TestReducer extends Reducer<Solver, Text, Solver, Text> {
		@Override
		protected void reduce(Solver key, Iterable<Text> values, Reducer<Solver, Text, Solver, Text>.Context context)
				throws IOException, InterruptedException {
			context.write(key, new Text(""));
		}
	}

	public static class ReadFirstOutputFileMap extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String readRow[] = value.toString().split("\\s");
			context.write(new Text(readRow[0]), new Text(readRow[1]));

		}
	}

	public static class TransformOutputReducer extends Reducer<Text, Text, Text, Text> {
		Map<String, ArrayList<String>> times = new HashMap<>();
		String keyL = "";

		@Override
		protected void reduce(Text argkey, Iterable<Text> argvalue, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			keyL = argkey.toString();
			if (!times.containsKey(keyL))
				times.put(keyL, new ArrayList<>());
			for (Text text : argvalue) {
				times.get(keyL).add(text.toString());
			}
		}

		@Override
		protected void cleanup(Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "TestMr");
		job.setJarByClass(TestMR.class);

		job.setMapperClass(TestMap.class);
		/// job.setCombinerClass(IntSumCombiner.class);
		job.setReducerClass(TestReducer.class);

		job.setOutputKeyClass(Solver.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "/iniziale"));
		boolean success = job.waitForCompletion(true);
		if (success) {
			Job job2 = Job.getInstance(conf, "job2");
			job2.setJarByClass(TestMR.class);

			job2.setMapperClass(ReadFirstOutputFileMap.class);
			/// job.setCombinerClass(IntSumCombiner.class);
			job2.setReducerClass(TransformOutputReducer.class);

			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);

			FileInputFormat.addInputPath(job2, new Path(args[1] + "/iniziale"));
			FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/finale"));
			success = job2.waitForCompletion(true);
		}
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
