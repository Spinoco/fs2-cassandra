package spinoco.fs2
import cats.effect.{Async, ContextShift}
import cats.syntax.all._
import com.google.common.util.concurrent.{FutureCallback, Futures, ListenableFuture}


package object cassandra {

  implicit def toAsyncF[F[_],A](f:ListenableFuture[A])(implicit F:Async[F], CS: ContextShift[F]): F[A] = {
    F.async[A] { cb =>
      Futures.addCallback(f, new FutureCallback[A] {
        def onFailure(t: Throwable): Unit = { cb(Left(t))}
        def onSuccess(result: A): Unit = { cb(Right(result)) }
      })
    } <* CS.shift
  }


}
