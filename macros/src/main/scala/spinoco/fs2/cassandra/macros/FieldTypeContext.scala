package spinoco.fs2.cassandra.macros

import scala.language.experimental.macros
import scala.reflect.macros.blackbox

/**
 * This allows us to define a case class to carry around the c.universe.Type with additional metadata.
 */
case class FieldTypeContext(c: blackbox.Context) {
  import c.universe._
  case class FieldType(tpe: c.universe.Type, key: String, idx: Int) {
    val typeTree: TypeTree = TypeTree(tpe)
    def varName(name: String, inc: Int = 0): TermName = {
      if (idx + inc >= 0) {
        TermName(s"$name${idx+inc}")
      } else {
        TermName(s"$name")
      }
    }
  }

  object FieldType {
    implicit class OptionFieldSyntax(self: Option[FieldType]) {
      def varName(name: String, inc: Int = 0): TermName = {
        self
          .map(field => field.idx + inc)
          .filter(_ >= 0)
          .map(idx => TermName(s"$name$idx"))
          .getOrElse(TermName(s"$name"))
      }
    }
  }
}
