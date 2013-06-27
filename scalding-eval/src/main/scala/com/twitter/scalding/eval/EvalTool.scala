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
import com.twitter.scalding.{ Tool, Args, Job, Mode, Local, Hdfs }
import com.twitter.util.Eval
import org.slf4j.{ Logger, LoggerFactory }

class EvalTool extends Tool {
  import EvalTool._

  private def getTmpJarPath(args: Args): Path =
    new Path(getConf.get("hadoop.tmp.dir"), args(Mode.MODE_KEY) + ".jar")

  private def compileJob(jobFile: File, tmpDir: Option[File] = None): Args => Job =
    new Eval(tmpDir).apply[Args => Job](jobFile)

  private def putJarOnHdfs(tmpDir: File, tmpJarPath: Path) {
    val jarOut = new JarOutputStream(tmpJarPath.getFileSystem(getConf).create(tmpJarPath, false))
    tmpDir.listFiles.foreach{ f => {
      jarOut.putNextEntry(new JarEntry(f.getName))
      val fileIn = new FileInputStream(f)
      ByteStreams.copy(fileIn, jarOut)
      fileIn.close()
      f.delete()
    }}
    jarOut.close()
  }

  private def getHdfsJob(jobFilename: String, args: Args): Job = {
    // create local temporary dir to compile to
    val tmpDir = Files.createTempDir()
    logger.info("using local temporary dir {}", tmpDir)

    try {
      val job = compileJob(new File(jobFilename), Some(tmpDir))(args)

      // create temporary jar on hdfs
      val tmpJarPath = getTmpJarPath(args)
      logger.info("using hfs temporary file {}", tmpJarPath)
      putJarOnHdfs(tmpDir, tmpJarPath)

      // add temporary jar to distributed cache
      DistributedCache.addFileToClassPath(tmpJarPath, getConf)

      job
    } finally {
      tmpDir.listFiles.foreach{ f => f.delete() }
      logger.info("deleting temporary dir {}", tmpDir)
      tmpDir.delete()
    }
  }

  private def getLocalJob(jobFilename: String, args: Args): Job =
    compileJob(new File(jobFilename))(args)

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
      Mode.getMode(args).get match {
        case l: Local => getLocalJob(jobFilename, nonJobNameArgs)
        case h: Hdfs => getHdfsJob(jobFilename, nonJobNameArgs)
      }
    }
  }

  override def run(args : Array[String]): Int = {
    val (mode, jobArgs) = parseModeArgs(args)
    val jobArgsWithMode = Mode.putMode(mode, jobArgs)
    // Connect mode with job Args
    try {
      run(getJob(jobArgsWithMode))
    } finally {
      mode match {
        case l: Local => {
          logger.debug("no cleanup necessary for local mode")
        }
        case h: Hdfs => {
          val tmpJarPath = getTmpJarPath(jobArgsWithMode)
          logger.info("deleting hfs temporary file {}", tmpJarPath)
          tmpJarPath.getFileSystem(getConf).delete(tmpJarPath, false)
        }
      }
    }
  }

}

object EvalTool {
  private val logger = LoggerFactory.getLogger(classOf[EvalTool])

  def main(args: Array[String]) {
    ToolRunner.run(new Configuration, new EvalTool, args)
  }
}
