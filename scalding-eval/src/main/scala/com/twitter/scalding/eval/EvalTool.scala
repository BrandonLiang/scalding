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

import java.util.jar.{ JarOutputStream, JarEntry }
import java.io.{ File, FileInputStream }
import com.google.common.io.{ Files, ByteStreams }
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.filecache.DistributedCache
import org.apache.hadoop.util.ToolRunner
import com.twitter.scalding.{ Tool, Args, Job, Mode }
import com.twitter.util.Eval
import org.slf4j.{ Logger, LoggerFactory }

class EvalTool extends Tool {
  import EvalTool._

  private def getTmpJarPath(job: Job) = new Path(getConf.get("hadoop.tmp.dir"), job.args(Mode.MODE_KEY) + ".jar")

  override protected def getJob(args: Args): Job = {
    if(rootJob.isDefined) {
      rootJob.get.apply(args)
    }
    else if(args.positional.isEmpty) {
      sys.error("Usage: EvalTool <jobFile> --local|--hdfs [args...]")
    }
    else {
      val jobFilename = args.positional(0)
      // Remove the job filename from the positional arguments:
      val nonJobNameArgs = args + ("" -> args.positional.tail)

      // create local temporary dir to compile to
      val tmpDir = Files.createTempDir()
      logger.info("using local temporary dir {}", tmpDir)

      try {
        // compile job
        val job = new Eval(Some(tmpDir)).apply[Function1[Args, Job]](new File(jobFilename))(nonJobNameArgs)

        // create temporary jar on hdfs
        val tmpJarPath = getTmpJarPath(job)
        logger.info("using hfs temporary file {}", tmpJarPath)
        val jarOut = new JarOutputStream(tmpJarPath.getFileSystem(getConf).create(tmpJarPath, false))
        for (f <- tmpDir.listFiles()) {
          jarOut.putNextEntry(new JarEntry(f.getName))
          val fileIn = new FileInputStream(f)
          ByteStreams.copy(fileIn, jarOut)
          fileIn.close()
          f.delete()
        }
        jarOut.close()

        // add temporary jar to distributed cache
        DistributedCache.addFileToClassPath(tmpJarPath, getConf)

        job
      } finally {
        for (f <- tmpDir.listFiles())
          f.delete()
        logger.info("deleting temporary dir {}", tmpDir)
        tmpDir.delete()
      }
    }
  }

  override protected def run(job : Job) : Int = {
    try {
      super.run(job)
    } finally {
      val tmpJarPath = getTmpJarPath(job)
      logger.info("deleting hfs temporary file {}", tmpJarPath)
      tmpJarPath.getFileSystem(getConf).delete(tmpJarPath, false)
    }
  }

}

object EvalTool {
  private val logger = LoggerFactory.getLogger(classOf[EvalTool])

  def main(args: Array[String]) {
    ToolRunner.run(new Configuration, new EvalTool, args)
  }
}
