package com.dilara.spark.app;



import com.google.common.collect.Iterators;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

// import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Iterator;


public class App {
    public static void main(String[] args) {


        System.setProperty("hadoop.home.dir","C:\\hadoop-common-2.2.0-bin-master");

        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("MongoSpark")
                .config("spark.mongodb.input.uri","mongodb://127.0.0.1/test.WordCupCollection")
                .config("spark.mongodb.output.uri","mongodb://127.0.0.1/test.WordCupCollection")
                .getOrCreate();


        JavaSparkContext sc=new JavaSparkContext(spark.sparkContext());

        //JavaSparkContext sc = new JavaSparkContext("local","Map Func");
        JavaRDD<String> Raw_Data = sc.textFile("C:\\Users\\Dilara\\Desktop\\WorldCup\\WorldCupPlayers.csv");

       //  System.out.println(Raw_Data.count());

        final JavaRDD<PlayersModel> playerdRDD = Raw_Data.map(new Function<String, PlayersModel>() {

            public PlayersModel call(String line) throws Exception {
                String[] dizi = line.split(",",-1);
                return new PlayersModel(dizi[0], dizi[1]
                        , dizi[2], dizi[3], dizi[4], dizi[5], dizi[6], dizi[7], dizi[8]);
            }
        });

        playerdRDD.filter(new Function<PlayersModel, Boolean>() {
            public Boolean call(PlayersModel playersModel) throws Exception {
                return playersModel.getTeam().equals("TUR");
            }
        });

       /* playerdRDD.foreach(new VoidFunction<PlayersModel>() {
            public void call(PlayersModel playersModel) throws Exception {
                System.out.println(playersModel.getPlayerName());
            }
        }); */

       // messi dünya kupası kaç yaptı_?
        /* JavaRDD<PlayersModel> messiRDD = playerdRDD.filter(new Function<PlayersModel, Boolean>() {
            public Boolean call(PlayersModel playersModel) throws Exception {
                return playersModel.getPlayerName().equals("MESSI");
            }
        });
        System.out.println(" Messi dünya kupalarında " + messiRDD.count()+" maç yaptı"); */

        //   Futbolcunun adı 35
        JavaPairRDD<String, String> mapRDD = playerdRDD.mapToPair(new PairFunction<PlayersModel, String, String>() {
            public Tuple2<String, String> call(PlayersModel playersModel) throws Exception {
                return new Tuple2<String, String>(playersModel.getPlayerName(), playersModel.getMatchID());
            }
        });

      /* mapRDD.foreach(new VoidFunction<Tuple2<String, String>>() {
          public void call(Tuple2<String, String> line) throws Exception {
              System.out.println(line._1 + " " + line._2);
          }
      }); */


        JavaPairRDD<String, Iterable<String>> groupPlayer = mapRDD.groupByKey();

       /* groupPlayer.foreach(new VoidFunction<Tuple2<String, Iterable<String>>>() {
            public void call(Tuple2<String, Iterable<String>> line) throws Exception {
                System.out.println(line._1 + " " + line._2);
            }
        });
*/

        JavaRDD<com.dilara.spark.app.groupPlayer> resulutRDD = groupPlayer.map(new Function<Tuple2<String, Iterable<String>>, groupPlayer>() {


            public com.dilara.spark.app.groupPlayer call(Tuple2<String, Iterable<String>> dizi) throws Exception {
                Iterator<String> iteratorraw = dizi._2().iterator();
                int size = Iterators.size(iteratorraw);
                return new groupPlayer(dizi._1, size);
            }
        });

        resulutRDD.foreach(new VoidFunction<com.dilara.spark.app.groupPlayer>() {
            public void call(com.dilara.spark.app.groupPlayer groupPlayer) throws Exception {
                System.out.println(groupPlayer.getPlayerName() + " " + groupPlayer.getMatchCount());
            }
        });

    }


}
