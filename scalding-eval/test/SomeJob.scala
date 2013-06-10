import com.twitter.scalding._

(args : Args) => {
  new Job(args) {
    TextLine(args("input"))
      .flatMap('line -> 'word) { line : String => line.split("\\s+") }
      .groupBy('word) { _.size }
      .write(Tsv(args("output")))
  }
}
