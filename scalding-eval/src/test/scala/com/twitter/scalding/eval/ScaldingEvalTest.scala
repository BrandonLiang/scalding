/*
Copyright 2012 Tresata, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package com.twitter.scalding.eval

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.JobConf
import com.twitter.scalding.{ JobTest, Args, Job, Local, Hdfs, TextLine, Tsv }
import org.specs._

class ScaldingEvalJob extends Specification {

  // not used but another attempt to fix sbt versus util-eval classloader issues
  private def withClassLoader(klass: Class[_])(expr: Unit) {
    val cl = Thread.currentThread.getContextClassLoader
    try {
      Thread.currentThread.setContextClassLoader(klass.getClassLoader)
      expr
    } finally {
      Thread.currentThread.setContextClassLoader(cl)
    }
  }

  "A ScaldingEval" should {
    val jobString = """
      import com.twitter.scalding._

      (args : Args) => {
        new Job(args) {
          TextLine(args("input"))
            .flatMap('line -> 'word) { line : String => line.split("\\s+") }
            .groupBy('word) { _.size }
            .write(Tsv(args("output")))
        }
      }
      """

    val input = List((1, "this is a test"), (2, "this is another test"))
    val expected = Map("this" -> 2, "is" -> 2, "a" -> 1, "test" -> 2, "another" -> 1)

    "create a runnable Job in local mode" in {
      val eval = ScaldingEval[Args => Job](jobString)(Local(false))
      JobTest(eval.get)
        .arg("input","fakeInput")
        .arg("output","fakeOutput")
        .source(TextLine("fakeInput"), input)
        .sink[(String, Long)](Tsv("fakeOutput")) { outBuf => {
          val results = outBuf.toMap
          results must be_==(expected)
        }}
        .run
        .finish
    }

    // not working due to class loader issues
    /**
    "create a runnable Job in hadoop mode" in {
      val conf = new Configuration()
      val eval = ScaldingEval[Args => Job](jobString)(Hdfs(false, conf))
      JobTest(eval.get)
        .arg("input","fakeInput")
        .arg("output","fakeOutput")
        .source(TextLine("fakeInput"), input)
        .sink[(String, Long)](Tsv("fakeOutput")) { outBuf => {
          val results = outBuf.toMap
            results must be_==(expected)
        }}
        .runHadoopWithConf(new JobConf(conf))
        .finish
    }
    **/
  }

}
