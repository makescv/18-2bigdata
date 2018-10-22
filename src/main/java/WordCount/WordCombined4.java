package WordCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


public class WordCombined4 {


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



    //////R2매퍼들 모음


    //R1에서 추린거 읽는거(5명) //이거 거의 비슷하네?
    //output: 사람, -1

    //원본 읽는거
    //output: 사람, 지역
    public static class R2mapper1 extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
        IntWritable intWritable = new IntWritable();
        IntWritable intWritable2 = new IntWritable();
        IntWritable minus = new IntWritable(-1);//R1에서 추린 5명에 해당한다는걸 표현함//독약같은거
        int top5[]=new int[100];

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //입력 처리
            String line = value.toString();
            String word[]=line.split("\\W+");
            //System.out.println("---------------------------");
            //System.out.println(key.get());
            if (word.length>2){//오리지널 인풋 처리
                int id,loc;
                id=Integer.parseInt(word[0]);
                loc=(int)word[3].charAt(0)-(int)'A';//알파벳 A부터 0으로
                intWritable.set(loc);
                intWritable2.set(id);
                context.write(intWritable, intWritable2);// 지역 사람
            }else{ //R1(top5)처리//여기를 reducer의 setup으로 하면 좋을거같은데 어짜피 1개짜리라면
                int id;
                id=Integer.parseInt(word[1]);//지역
                intWritable.set(id);
                context.write(intWritable, minus);
//                top5[id]=1;//기록
            }
        }
    }

    //id -1AABBDDEE <-top5인놈
    //id AAD <-아닌놈
    public static class R2SumReducer extends Reducer<IntWritable, IntWritable, IntWritable, Text> {
        IntWritable intWritable = new IntWritable();
        int locCount[]=new int[100];//100 상수처리, 값을 정하자

        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int isTop5=0;
            ArrayList<Integer> arr=new ArrayList<Integer>();
            for(IntWritable value:values) {
                System.out.println(value.get());//디버
                if (value.get() == -1) isTop5=1;
                else arr.add(value.get());
            }
            if (isTop5==0) return;
            System.out.println("key!");
            System.out.println(key.toString());
            System.out.println(arr.size());

            //중복제거... 비효율적이지만 시간이 없어서
//            List<Integer> al = new ArrayList<Integer>();
            // add elements to al, including duplicates
            Set<Integer> hs = new HashSet<Integer>();
            hs.addAll(arr);
            arr.clear();
            arr.addAll(hs);

            //strbuffer가 자주 수정될때는 더 빠르뎃나?
            String dd="";

            for(int value:arr) {
                dd+=value+" ";
                //System.out.println(value);
                //디버그 목적
                //locCount[value]=locCount[value]+1;
            }
            context.write(key, new Text(dd));
            return;
        }
    }


/*

    public int run(String[] args) throws Exception {

        JobControl jobControl = new JobControl("jobChain");
        Configuration conf1 = getConf();

        //job1
        Job job1 = Job.getInstance(conf1);
        job1.setJarByClass(WordCombined2.class);
        job1.setJobName("Word Combined");


        //job1.setMapperClass(WordCombined2.R2mapper1.class);
        job1.setReducerClass(WordCombined2.R2SumReducer.class);
        job1.setCombinerClass(WordCombined2.R2SumReducer.class);

        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(IntWritable.class);

        MultipleInputs.addInputPath(job1, new Path(args[1] + "/R1"), TextInputFormat.class, R2mapper1.class);
        MultipleInputs.addInputPath(job1, new Path(args[0]),TextInputFormat.class, R2mapper1.class);
        //FileInputFormat.setInputPaths(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1] + "/temp2"));

        ControlledJob controlledJob1 = new ControlledJob(conf1);
        controlledJob1.setJob(job1);


        //job2고고! R1정렬해주는거!
        Configuration conf2 = getConf();

        Job job2 = Job.getInstance(conf2);
        job2.setJarByClass(WordCombined2.class);
        job2.setJobName("Word Invert");

        FileInputFormat.setInputPaths(job2, new Path(args[1] + "/temp2"));
        FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/R2-final"));

        job2.setMapperClass(WordCombined2.R2WordMapper2.class);
        job2.setReducerClass(WordCombined2.R2SumReducer2.class);
        job2.setCombinerClass(WordCombined2.R2SumReducer2.class);

        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(IntWritable.class);
        job2.setInputFormatClass(KeyValueTextInputFormat.class);

        job2.setSortComparatorClass(WordCombined2.IntComparator.class);
        ControlledJob controlledJob2 = new ControlledJob(conf2);
        controlledJob2.setJob(job2);

        // make job2 dependent on job1
        controlledJob2.addDependingJob(controlledJob1);


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
        int exitCode = ToolRunner.run(new WordCombined2(), args);
        System.exit(exitCode);
    }
*/

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = new Job(conf, "Reduce-side join");
        job.setJarByClass(WordCombined4.class);
        job.setReducerClass(R2SumReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        MultipleInputs.addInputPath(job, new Path(args[1] + "/R2-final"),TextInputFormat.class, R2mapper1.class);
        MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class, R2mapper1.class);
        Path outputPath = new Path(args[1] + "/temp3");

        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}


