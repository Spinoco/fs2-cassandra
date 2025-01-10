package spinoco

import scodec.codecs.liftF3ToNestedTupleF
import spinoco.KarelsTweaks.ColorPrinter.printColor
import spinoco.KarelsTweaks.KotlinSyntax.KotlinSyntax
import spinoco.KarelsTweaks.TypoAliases.printk

import java.time.Instant
import scala.collection.mutable

object KarelsTweaks {
  object KotlinSyntax {
    implicit class KotlinSyntax[R](val self: R) extends AnyVal {
      def also(f: R => Any): R = {
        f(self)
        self
      }

      def let[O](f: R => O): O = {
        f(self)
      }
    }
  }

  object TypoAliases {
    def printk(s: String) = println(s)
  }

  object Color extends Enumeration {
    val Red, Green, Yellow, Blue, Orange, Violet, Brown, LightBlue, Gray = Value
  }

  object ColorPrinter {
    def getTimePrefix(): String = {
      s"${Instant.now().getEpochSecond % 1000}"
    }

    def printColor(color: Color.Value, text: String): Unit = {
      val colorCode = color match {
        case Color.Red => "\u001b[31m"
        case Color.Green => "\u001b[32m"
        case Color.Yellow => "\u001b[33m"
        case Color.Blue => "\u001b[34m"
        case Color.Orange => "\u001b[38;5;208m"
        case Color.Violet => "\u001b[35m"
        case Color.Brown => "\u001b[38;5;130m"
        case Color.LightBlue => "\u001b[38;5;45m"
        case Color.Gray => "\u001b[90m"
      }
      println(s"${getTimePrefix()}${colorCode}${text}\u001b[0m")
    }
  }

  object CallTracer {
    private var contextStack: Option[TraceCtx] = None

    case class TraceCtx(message: String, depth: Int = 0, parent: Option[TraceCtx] = None,   isJump: Boolean = false, withLogs: Boolean = false) {
      def jump: TraceCtx = copy(isJump = true)
    }

    def LOGMSG[R](msg: String)(f: => R): R = {
      impl.callScope(impl.getNewContext(+1, msg), f)
    }

    def LOG(msg: String, increment: Int = 0): Unit = {
      impl.justLog(impl.getNewContext(+1+increment, msg))
    }

    def LOGCALL[R](f: => R): R = {
      val call = Thread.currentThread.getStackTrace()(2)
      val msg = s"${call.getClassName.replaceAll(".*[.]","")}.${call.getMethodName}:${call.getLineNumber}"
      impl.callScope(impl.getNewContext(+1, msg), f)
    }

    def LOGCALL[R](inc: Int)(f: => R): R = {
      val call = Thread.currentThread.getStackTrace()(2+inc)
      val msg = s"${call.getClassName.replaceAll(".*[.]","")}.${call.getMethodName}:${call.getLineNumber}"
      impl.callScope(impl.getNewContext(+1, msg), f)
    }

    def WITHLOGS[R](f: => R): R = {
      val call = Thread.currentThread.getStackTrace()(2)
      val msg = s"withlogs at ${call.getClassName.replaceAll(".*[.]","")}.${call.getMethodName}:${call.getLineNumber}"
      impl.callScope(impl.getNewContext(+1, msg).copy(withLogs = true), f)
    }

    def LOGWITH[R](ctx: TraceCtx, appendMsg: String)(f: => R): R = {
      impl.callScope(ctx.copy(isJump = true, message = s"${ctx.message}|$appendMsg"), f)
    }

    def LOGCTX(l: Int = 0, h: Int = 0): TraceCtx = {
      val msg = (l to h).map { i =>
        val call = Thread.currentThread.getStackTrace()(9+i)
        val msg = s"${call.getClassName.replaceAll(".*[.]","")} ${call.getMethodName}:${call.getLineNumber}"
        msg
      }.mkString("(","+",")")
      impl.getNewContext(+1, msg)
    }

    object impl {
      def getNewContext(increment: Int, msg: String): TraceCtx = {
        val parent = CallTracer.contextStack.getOrElse(TraceCtx("", 0))
        val depth = parent.depth + increment
        parent.copy(message = msg, depth = depth+increment, parent = Some(parent))
      }

      def getPrefix(ctx: TraceCtx): String = {
        val spacer = if (ctx.isJump)  "---" else "   "
        (0 until ctx.depth).map(_ => spacer).mkString("")
      }

      def printTrace(prefix: String, e: Throwable, long: Boolean): Unit = {
        if (long) {
          e.printStackTrace()
        } else {
          printColor(Color.Red, s"$prefix- ${e.getClass.getName.replaceAll(".*[.]", "")}: ${e.getMessage}")
          if (e.getCause != null) {
            printTrace(prefix, e.getCause, long)
          }
        }
      }

      def callScope[R](ctx: TraceCtx, f: => R): R = {
        val prefix = getPrefix(ctx)
        val msg = ctx.message
        if (ctx.withLogs) {
          printColor(Color.Yellow, s"$prefix-> $msg")
        }
        CallTracer.contextStack = Some(ctx)
        try {
          val res = f
          if (ctx.withLogs) {
            printColor(Color.Yellow, s"$prefix<- $msg")
          }
          res
        }
        catch {
          case e: Throwable =>
            printColor(Color.Red, s"$prefix   $msg FAILED(brief):")
            printTrace(s"$prefix   ", e, false)
            printColor(Color.Red, s"$prefix   $msg FAILED(full):")
            printTrace(s"$prefix   ", e, true)
//            Thread.sleep(10000)
            printColor(Color.Yellow, s"$prefix!! $msg FAILED(rethrow)")
            throw e
        }
        finally {
          CallTracer.contextStack = ctx.parent
        }
      }

      def justLog(ctx: TraceCtx): Unit = {
        val prefix = getPrefix(ctx)
        val msg = ctx.message
        if (ctx.withLogs) {
          printColor(Color.Yellow, s"$prefix$msg")
        }
      }
    }
  }
}
