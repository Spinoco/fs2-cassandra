package spinoco.fs2.cassandra.macros

import scala.reflect.macros.blackbox

/**
 * This defines a macro that generates a CTypeRecord for a given labelled HList in one go.
 *
 * This speeds up compilation, as it saves the compiler from having to compare types on recursive implicit searches.
 */
object CTypeRecordGenerator {
  def generate[L: c.WeakTypeTag](c: blackbox.Context): c.Expr[CTypeRecord[L]] = {
    val fc = FieldTypeContext(c)
    val impl = ImplementationContext(fc)
    impl.generate[L].asInstanceOf[c.Expr[CTypeRecord[L]]]
  }

  case class ImplementationContext(fc: FieldTypeContext) {
    import fc.FieldType
    import fc.c.universe._

    def createCacheFor(name: TermName, field: FieldType): Tree = {
      q"""
        val $name: CType[${field.typeTree}] = implicitly[CType[${field.typeTree}]]
      """
    }

    def extractValues(typeList: List[FieldType], name: String): List[Tree] = typeList.flatMap { field =>
      if (field.idx == 0) {
        List(
          q"val ${field.varName(name)} = r.head",
          q"val ${field.varName("tail")} = r.tail"
        )
      } else {
        List(
          q"val ${field.varName(name)} = ${field.varName("tail", -1)}.head",
          q"val ${field.varName("tail")} = ${field.varName("tail", -1)}.tail"
        )
      }
    }

    def constructHlist(typeList: List[FieldType], resTpe: Tree, name: fc.FieldType => TermName) = {
      val simpleHlistInstance = typeList
        .map { field => name(field) }
        .foldRight(q"shapeless.HNil": Tree) { (term, acc) =>
          q"shapeless.::($term, $acc)"
        }

      q"""Right($simpleHlistInstance)"""
    }

    def monadicFor(typeList: List[FieldType], name: FieldType => TermName, monadValue: FieldType => Tree)(innerBody: => Tree): Tree = {
      typeList.foldRight(innerBody) { case (field: FieldType, acc) =>
        q"""
          ${monadValue(field)}
           .flatMap { tmp =>
             val ${name(field)}: shapeless.labelled.FieldType[${field.labelTree}, ${field.typeTree}] = shapeless.labelled.field[${field.labelTree}][${field.typeTree}](tmp)
             $acc
           }
         """
      }
    }

    def generate[L: fc.c.WeakTypeTag]: fc.c.Expr[CTypeRecord[L]] = {
      import fc.c.universe._

      type FieldType = fc.FieldType

      val tpe = weakTypeOf[L]
      val extractor = HListExtractor(fc)
      val typeList = extractor.toLabeledTypeList(tpe.asInstanceOf[extractor.fc.c.universe.Type]).asInstanceOf[List[FieldType]]

      val hlistTpe = weakTypeOf[L]
      val hlistTpeTree = tq"$hlistTpe"
      val explicitTypeTree = AppliedTypeTree(
        Ident(typeOf[CTypeRecord[_]].typeSymbol),
        List(TypeTree(tpe))
      )

      val extractedVals = extractValues(typeList, "value")

      val implDef =
        q"""
      new ${explicitTypeTree} {
        import spinoco.fs2.cassandra.ctype._
        import java.nio.ByteBuffer
        import com.datastax.oss.driver.api.core.ProtocolVersion
        import com.datastax.oss.driver.api.core.data.{GettableByName, SettableByName}
        import shapeless._
        import shapeless.labelled._
        import shapeless.tag._
        import shapeless.tag.Tagged
        import shapeless.syntax.singleton._
        import com.datastax.oss.driver.api.core.`type`.DataType

        ..${
          typeList
          .map(field => createCacheFor(field.varName("cache"), field))
        }

        type CTypes = ${TypeTree(tpe)}

        def types: Seq[(String, DataType)] = {
          ${
            typeList
            .map { field => q"""${field.key} -> ${field.varName("cache")}.cqlType""" }
            .fold(q"""Seq.empty[(String, DataType)]"""){ (acc, seq) => q"""$acc :+ $seq""" }
          }
        }

        def writeCql(r: $hlistTpeTree): Map[String, String] = {
          ..$extractedVals
          ${
            typeList
            .map { field => q"""${field.varName("cache")}.writeCql(${field.key},${field.varName("value")})""" }
            .fold(q"""Map.empty[String, String]""") { (acc, dict) => q"""$acc ++ $dict""" }
          }
        }

        def writeRaw(r: $hlistTpeTree, protocolVersion: ProtocolVersion): Map[String, ByteBuffer] = {
          ..$extractedVals
          ${
            typeList
            .map { field => q"""${field.varName("cache")}.writeRaw(${field.key},${field.varName("value")}, protocolVersion)""" }
            .fold(q"""Map.empty[String, ByteBuffer]""") { (acc, dict) => q"""$acc ++ $dict""" }
          }
        }

        def writeByName[D <: SettableByName[D]](r: $hlistTpeTree, data: D, protocolVersion: ProtocolVersion): D = {
          ..$extractedVals
          ..${
            typeList
            .map { field => q"""val ${field.varName("data")} = ${field.varName("cache")}.writeByName(${field.key}, ${field.varName("value")}, ${field.varName("data", -1)}, protocolVersion)""" }
          }
          ${typeList.lastOption.varName("data")}
        }

        def readByName(data: GettableByName, protocolVersion: ProtocolVersion): Either[Throwable, $hlistTpeTree] = {
          ${
            monadicFor(
              typeList,
              name = field => field.varName("value"),
              monadValue = field => q""" ${field.varName("cache")}.readByName(${field.key}, data, protocolVersion)"""
            ) {
              constructHlist(typeList = typeList, resTpe = hlistTpeTree, name = field => field.varName("value"))
            }
          }
        }

        def readByNameIfExists(keys: Set[String], data: GettableByName, protocolVersion: ProtocolVersion): Either[Throwable, $hlistTpeTree] = {
          ${
            monadicFor(
              typeList,
              name = field => field.varName("value"),
              monadValue = field => q""" ${field.varName("cache")}.readByNameIfExists(keys, ${field.key}, data, protocolVersion)"""
            ) {
              constructHlist(typeList = typeList, resTpe = hlistTpeTree, name = field => field.varName("value"))
            }
          }
        }
      }
      """

      fc.c.Expr[CTypeRecord[L]](implDef)
    }
  }
}
