/**
 * Author: Derek Lin
 * Last Modified: Nov 22, 2019
 *
 * This program demonstrates the spark and utilization of RDD.
 * We will implement the function of counting line function in task0, the
 * function of counting words in task1, the function of counting distinct
 * words in task2. After we will output the word mapping in the relevant tasks.
 * Moreover, implementing reducer function of RDD.
 */
package edu.cmu.yunanl;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.util.Arrays;
import java.util.Scanner;

public class TempestAnalytics {
    private static void taskSolver(String fileName) {
        //initialize the spark config object
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("JD Word Counter");
        //bind the spark config object to context object
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        //utilize the context object to parse the input file
        JavaRDD<String> inputFile = sparkContext.textFile(fileName);
        //Task0
        //print out the outcome of number of lines in the file
        System.out.println("Task0: Number of lines: " + inputFile.count());
        //implement RDD function to split the word from every line
        JavaRDD<String> wordsFromFile = inputFile.flatMap(content -> Arrays.asList(content.split(" ")));
        //Task1
        //print out the outcome of number of words in the file
        System.out.println("Task1: Number of words: " + wordsFromFile.count());
        //Task2
        //print out the outcome of number of distinct words in the file
        System.out.println("Task2: Number of distinct words: " + wordsFromFile.distinct().count());
        //implement RDD function to map every word into 1 which is a pair of key and value
        JavaPairRDD countData = wordsFromFile.mapToPair(t -> new Tuple2(t, 1));
        //Task3
        //output the file to indicated path
        countData.saveAsTextFile("TheTempestOutputDir1");
        //implement RDD function to reduce the specific key and add them all into a pair of key and value
        countData = countData.reduceByKey((x, y) -> (int) x + (int) y);
        //Task4
        //output the file to indicated path
        countData.saveAsTextFile("TheTempestOutputDir2");
        //Task5
        Scanner input = new Scanner(System.in);
        System.out.println("Please key in a word to search in The Tempest: ");
        //user will input a word to search in the file
        String request = input.nextLine();
        //loop every line in the file to search for the word contained
        inputFile.foreach(eachLine ->{
            if(eachLine.contains(request)) {
                System.out.println(eachLine);
            }
        });
    }

    public static void main(String[] args) {
        if (args.length == 0) {
            System.out.println("No files provided.");
            System.exit(0);
        }
        //provide the file by the argument
        taskSolver(args[0]);
    }
}
