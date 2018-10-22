package WordCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.nio.ByteBuffer;


public class WordCombined5 extends Configured implements Tool {

    //지역,1 처리
    public static class WordMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
        IntWritable intWritable = new IntWritable();
        IntWritable one = new IntWritable(1);

        @Override //아마 키가 줄수지
        //지역, 1로 만듬
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //입력 처리
            String line = value.toString();
            String word[]=line.split("\\W+");

            IntWritable k=new IntWritable(Integer.parseInt(word[0]));//키벨류인풋으로 해도됨
            IntWritable v=new IntWritable(Integer.parseInt(word[1]));//키벨류인풋으로 해도됨
            context.write(k, v);
        }
    }

    //<지역,1>을 합계하자
    public static class SumReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        IntWritable intWritable = new IntWritable();

        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int cnt=0;
            for(IntWritable value:values){
                //context.write(key,new IntWritable(cnt));//디버그
/*                cnt += 1;
                value.get();*///이거는 오류가나고///////////////////////////////////왜?
                cnt += value.get();
            }
            intWritable.set(cnt);
            context.write(key,intWritable);//로컬 cnt로 합계를 내면 안됨
            /*for (IntWritable value : values) {//동점자 스테이블 문제가 발생하겟다 ㅠㅠ 일단 무시
                if (count++<4)context.write(key,value);
            }*/
        }
    }


    //벨류로 정렬해준다!
    //단순 키벨류 교환
    public static class WordMapper2 extends Mapper< Text, Text, IntWritable, IntWritable> {
        //IntWritable frequency = new IntWritable();

        @Override
        public void map(Text key, Text value, Context context)
                throws IOException, InterruptedException {
            int k1 = Integer.parseInt(key.toString());
            int v1 = Integer.parseInt(value.toString());
            IntWritable k2=new IntWritable(v1);//크로스 해서 정렬시키자
            IntWritable v2=new IntWritable(k1);
            context.write(k2, v2);
        }
    }

    public static class SumReducer2 extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        int count=0;
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            for (IntWritable value : values) {//동점자 스테이블 문제가 발생하겟다 ㅠㅠ 일단 무시
                if (count++<4)context.write(key,value);
            }
        }
    }

    public static class IntComparator extends WritableComparator {

        public IntComparator() {
            super(IntWritable.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2,
                           int s2, int l2) {
            Integer v1 = ByteBuffer.wrap(b1, s1, l1).getInt();
            Integer v2 = ByteBuffer.wrap(b2, s2, l2).getInt();
            return v1.compareTo(v2) * (-1);
        }
    }


    public int run(String[] args) throws Exception {

        JobControl jobControl = new JobControl("jobChain");
        Configuration conf1 = getConf();

        //job1
        Job job1 = Job.getInstance(conf1);
        job1.setJarByClass(WordCombined5.class);
        job1.setJobName("Word Combined");

        FileInputFormat.setInputPaths(job1, new Path(args[1] + "/temp3"));
        FileOutputFormat.setOutputPath(job1, new Path(args[1] + "/R3"));

        job1.setMapperClass(WordMapper.class);
        job1.setReducerClass(SumReducer.class);
        job1.setCombinerClass(SumReducer.class);

        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(IntWritable.class);

        job1.setSortComparatorClass(IntComparator.class);

        ControlledJob controlledJob1 = new ControlledJob(conf1);
        controlledJob1.setJob(job1);


        //job2고고! R1정렬해주는거!
        Configuration conf2 = getConf();

        Job job2 = Job.getInstance(conf2);
        job2.setJarByClass(WordCombined5.class);
        job2.setJobName("Word Invert");

        FileInputFormat.setInputPaths(job2, new Path(args[1] + "/R3"));
        FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/R3-final"));

        job2.setMapperClass(WordMapper2.class);
        job2.setReducerClass(SumReducer2.class);
        job2.setCombinerClass(SumReducer2.class);

        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(IntWritable.class);
        job2.setInputFormatClass(KeyValueTextInputFormat.class);

        job2.setSortComparatorClass(IntComparator.class);
        ControlledJob controlledJob2 = new ControlledJob(conf2);
        controlledJob2.setJob(job2);

        // make job2 dependent on job1
        controlledJob2.addDependingJob(controlledJob1);

        //controlledJob3.addDependingJob(controlledJob1);

        // add the job to the job control
        jobControl.addJob(controlledJob1);
        jobControl.addJob(controlledJob2);

        Thread jobControlThread = new Thread(jobControl);
        jobControlThread.start();

        while (!jobControl.allFinished()) {
            System.out.println("Jobs in waiting state: " + jobControl.getWaitingJobList().size());
            System.out.println("Jobs in ready state: " + jobControl.getReadyJobsList().size());
            System.out.println("Jobs in running state: " + jobControl.getRunningJobList().size());
            System.out.println("Jobs in success state: " + jobControl.getSuccessfulJobList().size());
            System.out.println("Jobs in failed state: " + jobControl.getFailedJobList().size());
            try {
                Thread.sleep(5000);
            } catch (Exception e) {

            }

        }
        System.exit(0);
        return (job1.waitForCompletion(true) ? 0 : 1);
    }
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new WordCombined5(), args);
        System.exit(exitCode);
    }
}


