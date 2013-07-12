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

import java.util.UUID
import java.util.jar.{ JarOutputStream, JarEntry }
import java.io.{ File, FileInputStream }
import java.nio.charset.Charset
import com.google.common.io.{ Files, ByteStreams }
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.filecache.DistributedCache
import com.twitter.scalding.{ Mode, CascadingLocal, HadoopMode }
import com.twitter.util.Eval
import org.slf4j.{ Logger, LoggerFactory }

object ScaldingEval {
  def apply[A](code: String)(implicit mode: Mode): ScaldingEval[A] = mode match {
    case hm: HadoopMode => new HadoopScaldingEval(hm, code)
    case cl: CascadingLocal => new LocalScaldingEval(cl, code)
  }

  def apply[A](file: File)(implicit mode: Mode): ScaldingEval[A] = apply(Files.toString(file, Charset.forName("UTF8")))(mode)
}

abstract sealed class ScaldingEval[A] {
  protected def compile(code: String, tmpDir: Option[File] = None): A = new Eval(tmpDir).apply[A](code)
  def get: A
  def close()
}

object HadoopScaldingEval {
  private val logger = LoggerFactory.getLogger(classOf[HadoopScaldingEval[_]])

  private def putJarOnHdfs(conf: Configuration, tmpDir: File, tmpJarPath: Path) {
    val jarOut = new JarOutputStream(tmpJarPath.getFileSystem(conf).create(tmpJarPath, false))
    tmpDir.listFiles.foreach{ f =>
      jarOut.putNextEntry(new JarEntry(f.getName))
      val fileIn = new FileInputStream(f)
      ByteStreams.copy(fileIn, jarOut)
      fileIn.close()
    }
    jarOut.close()
  }
}

class HadoopScaldingEval[A](mode: HadoopMode, code: String) extends ScaldingEval[A] {
  import HadoopScaldingEval._

  val tmpDir = Files.createTempDir()
  val tmpJarPath = new Path(mode.jobConf.get("hadoop.tmp.dir"), UUID.randomUUID() + ".jar")
  logger.info("using local temporary dir {}", tmpDir)
  override val get = try{
    val result = compile(code, Some(tmpDir))
    logger.info("using hfs temporary file {}", tmpJarPath)
    putJarOnHdfs(mode.jobConf, tmpDir, tmpJarPath)
    DistributedCache.addFileToClassPath(tmpJarPath, mode.jobConf)
    result
  } catch {
    case e => {
      try {
        cleanup()
      } catch {
        case e => Unit
      }
      throw e
    }
  }

  private def cleanup() {
    logger.info("deleting hfs temporary file {}", tmpJarPath)
    tmpJarPath.getFileSystem(mode.jobConf).delete(tmpJarPath, false)

    Option(tmpDir.listFiles).map{ _.foreach{ f => f.delete() }}
    logger.info("deleting temporary dir {}", tmpDir)
    tmpDir.delete()
  }

  override def close() { cleanup() }
}

class LocalScaldingEval[A](mode: CascadingLocal, code: String) extends ScaldingEval[A] {
  override val get = compile(code, None)

  override def close() {}
}
