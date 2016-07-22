package reductions

import scala.annotation._
import org.scalameter._
import common._

object ParallelParenthesesBalancingRunner {

  @volatile var seqResult = false

  @volatile var parResult = false

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 40,
    Key.exec.maxWarmupRuns -> 80,
    Key.exec.benchRuns -> 120,
    Key.verbose -> true
  ) withWarmer(new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val length = 100000000
    val chars = new Array[Char](length)
    val threshold = 10000
    val seqtime = standardConfig measure {
      seqResult = ParallelParenthesesBalancing.balance(chars)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential balancing time: $seqtime ms")

    val fjtime = standardConfig measure {
      parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
    }
    println(s"parallel result = $parResult")
    println(s"parallel balancing time: $fjtime ms")
    println(s"speedup: ${seqtime / fjtime}")
  }
}

object ParallelParenthesesBalancing {

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def balance(chars: Array[Char]): Boolean = {
    def balanceIter(index: Int, stack: Int): Boolean = {
      if (stack < 0) {
        false
      } else if(index < chars.length) {
        chars(index) match {
          case '(' => balanceIter(index + 1, stack + 1)
          case ')' => balanceIter(index + 1, stack - 1)
          case _ => balanceIter(index + 1, stack)
        }
      } else if (stack == 0) {
        true
      } else {
        false
      }
    }

    balanceIter(0,0)
  }

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def parBalance(chars: Array[Char], threshold: Int): Boolean = {



    def traverse(idx: Int, until: Int, arg1: Int, arg2: Int): (Int, Int) = {
      if (idx >= until) (arg1, arg2)
      else chars(idx) match {
        case ')' => traverse(idx + 1, until, arg1 - 1, arg2 + 1)
        case '(' => traverse(idx + 1, until, arg1 + 1, arg2 - 1)
        case _ => traverse(idx + 1, until, arg1, arg2)
      }
    }

    def reduceRight(from: Int, until: Int): (Int, Int) = {
      if (until - from <= threshold) traverse(from, until, 0, 0)
      else {
        val middle = (from + until) / 2
        val ((left1, left2), (right1, right2)) = parallel(
          reduceRight(from, middle),
          reduceRight(middle, until))

        (left1 + right1, left2 + right2)
      }
    }

    def reduceLeft(from: Int, until: Int): (Int, Int) = {
      if (until - from <= threshold) {
        val (left, right) = traverse(from, until, 0, 0)
        if (left < 0) {
          (-1, -1)
        } else {
          (left, right)
        }
      } else {
        val middle = (from + until) / 2
        val ((left1, left2), (right1, right2)) = parallel(
          reduceLeft(from, middle),
          reduceRight(middle, until))

        if (left1 < 0) {
          (-1, -1)
        } else {
          (left1 + right1, left2 + right2)
        }
      }
    }

    reduceLeft(0, chars.length) == (0, 0)
  }

  // For those who want more:
  // Prove that your reduction operator is associative!

}
