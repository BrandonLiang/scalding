// run with:
// scripts/eval-tool scalding-eval/test/SomeJob.scala --local --input in.txt --output out.txt
// scripts/eval-tool scalding-eval/test/SomeJob.scala --hdfs --input in.txt --output out
import com.twitter.scalding._

(args : Args) => {
  new Job(args) {
    TextLine(args("input"))
      .flatMap('line -> 'word) { line : String => line.split("\\s+") }
      .groupBy('word) { _.size }
      .write(Tsv(args("output")))
  }
}
