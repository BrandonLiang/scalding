// run with:
// hadoop jar scalding-eval/target/scala-2.9.2/scalding-eval-assembly-0.8.5.jar scalding-eval/test/SomeJob.scala --hdfs --input in.txt --output out
// or:
// scripts/eval-tool scalding-eval/test/SomeJob.scala --hdfs --input candidates.txt --output out
import com.twitter.scalding._

(args : Args) => {
  new Job(args) {
    TextLine(args("input"))
      .flatMap('line -> 'word) { line : String => line.split("\\s+") }
      .groupBy('word) { _.size }
      .write(Tsv(args("output")))
  }
}
