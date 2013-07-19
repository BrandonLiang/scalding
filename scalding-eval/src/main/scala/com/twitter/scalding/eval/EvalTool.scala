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

import java.io.File
import org.apache.hadoop.util.ToolRunner

import com.twitter.scalding.{ Job, Tool, Mode, Args, RichXHandler }

class EvalTool extends Tool {
  override def run(args : Array[String]) : Int = {
    val (mode, evalArgs) = parseModeArgs(args)
    ScaldingEval.use[Args => Job, Int](new File(evalArgs.positional(0)), mode){ jc =>
      setJobConstructor(jc)
      // Connect mode with job Args
      val jobArgs = evalArgs + ("" -> evalArgs.positional.tail)
      run(getJob(Mode.putMode(mode, jobArgs)))
    }
  }

}

object EvalTool {
  def main(args: Array[String]) {
    try {
      ToolRunner.run(new EvalTool, args)
    } catch {
      case t: Throwable => {
         //create the exception URL link in GitHub wiki
         val gitHubLink = RichXHandler.createXUrl(t)
         val extraInfo = (if(RichXHandler().handlers.exists(h => h(t))) {
             RichXHandler.mapping(t.getClass) + "\n"
         }
         else {
           ""
         }) +
         "If you know what exactly caused this error, please consider contributing to GitHub via following link.\n" + gitHubLink

         //re-throw the exception with extra info 
         throw new Throwable(extraInfo, t)
      }
    }
  }
}
