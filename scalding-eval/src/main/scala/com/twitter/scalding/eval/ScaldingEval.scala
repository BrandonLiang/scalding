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
import java.io.{ File, FileInputStream, FileOutputStream }
import java.nio.charset.Charset
import com.google.common.io.{ Files, ByteStreams }
import org.apache.hadoop.conf.Configuration
import com.twitter.util.Eval
import com.twitter.scalding.{ Mode, HadoopMode }
import org.slf4j.{ Logger, LoggerFactory }

object ScaldingEval {
  private val logger = LoggerFactory.getLogger(classOf[ScaldingEval])

  private def stringFromFile(file: File): String = Files.toString(file, Charset.forName("UTF8"))

  def apply[A](code: String)(implicit mode: Mode): A = {
    val sEval = new ScaldingEval
    val a = sEval.compile[A](code)
    sEval.addToCache(mode)
    a
  }

  def apply[A](code: File)(implicit mode: Mode): A = apply[A](stringFromFile(code))(mode)

  def use[A, B](code: String, mode: Mode)(block: A => B): B = {
    var t: Option[Throwable] = None
    val sEval = new ScaldingEval
    try {
      val a = sEval.compile[A](code)
      mode match {
        case hm: HadoopMode => sEval.addToCache(hm.jobConf)
        case _ => Unit
      }
      block(a)
    } catch {
      case e => {
        t = Some(e)
        throw e
      }
    } finally {
      try {
        sEval.cleanup()
      } catch {
        case e => {
          if (t.isEmpty)
            throw e
        }
      }
    }
  }

  def use[A, B](code: File, mode: Mode)(block: A => B): B = use[A, B](stringFromFile(code), mode)(block)
}

class ScaldingEval {
  import ScaldingEval.logger

  val tmpDir = Files.createTempDir()
  val classDir = new File(tmpDir, "classes")
  classDir.mkdir()
  val eval = new Eval(Some(classDir))
  val tmpJar = new File(tmpDir, UUID.randomUUID() + ".jar")

  logger.info("using local temporary dir {}", tmpDir)
  
  def compile[A](code: String): A = eval.inPlace[A](code)

  private def createJar() {
    require(!tmpJar.exists(), "jar can only be created once")
    val jarOut = new JarOutputStream(new FileOutputStream(tmpJar))
    classDir.listFiles.foreach{ f =>
      jarOut.putNextEntry(new JarEntry(f.getName))
      val fileIn = new FileInputStream(f)
      ByteStreams.copy(fileIn, jarOut)
      fileIn.close()
    }
    jarOut.close()
  }

  private def addJarToCache(conf: Configuration) {
    val tmpJars = Option(conf.get("tmpjars")).map{ _.split(",").toSet }.getOrElse(Set.empty)
    conf.set("tmpjars", (tmpJars + ("file://" + tmpJar.getAbsolutePath())).mkString(","))
  }

  def addToCache(conf: Configuration) {
    createJar()
    addJarToCache(conf)
  }

  def addToCache(mode: Mode) {
    mode match {
      case hm: HadoopMode => addToCache(hm.jobConf)
      case _ => Unit
    }
  }

  def cleanup() {
    logger.info("deleting temporary dir {}", tmpDir)
    Option(classDir.listFiles).map{ _.foreach{ f => f.delete() }}
    tmpJar.delete()
    tmpDir.delete()
  }

}
