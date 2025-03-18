package spinoco.fs2.cassandra.macros

import shapeless._

import scala.annotation.tailrec
import scala.language.experimental.macros

case class HListExtractor(fc: FieldTypeContext) {
  import fc.c.universe._

  private val hnilSymbol = symbolOf[HNil]
  private val consSymbol = typeOf[shapeless.::[_, _]].typeSymbol
  private val fieldTypeSymbol = typeOf[shapeless.labelled.FieldType[_, _]].typeSymbol
  private val keyTagSymbol = typeOf[shapeless.labelled.KeyTag[_, _]].typeSymbol
  private val symbolType = typeOf[scala.Symbol].typeSymbol
  private val taggedType = typeOf[shapeless.tag.Tagged[_]].typeSymbol

  /** This converts a (possibly labelled) HList into a list of (possibly complex and labelled) types. */
  def toTypeList(tpe: fc.c.universe.Type): List[fc.c.universe.Type] = {
    @tailrec
    def recurse(tpe: fc.c.universe.Type, accum: List[fc.c.universe.Type]): List[fc.c.universe.Type] = {
      tpe.typeSymbol match {
        case t if t == consSymbol =>
          val headType = tpe.typeArgs.head
          val tailType = tpe.typeArgs.tail.head

          recurse(tailType, headType :: accum)

        case t if t == hnilSymbol =>
          accum

        case t if t == fieldTypeSymbol =>
          val tailType = tpe.typeArgs.tail.head
          recurse(tailType, accum)

        case _ =>
          fc.c.abort(fc.c.enclosingPosition, s"Got weird type, doesn't look like a Hlist: $tpe, as ${tpe.dealias} or ${tpe.dealias.dealias}, ${tpe.typeSymbol}, ${tpe.termSymbol}")
      }
    }

    if (tpe.typeSymbol == hnilSymbol) {
      Nil
    } else {
      recurse(tpe, Nil).reverse
    }
  }

  /** This pulls a string label out of the type. */
  def extractStringFromLiteral(litType: fc.c.universe.Type): String = {
    import fc.c.universe._

    def extractStringConstant(t: Type): Option[String] = {
      t match {
        case ConstantType(Constant(str: String)) => Some(str)
        case _ => None
      }
    }

    litType.dealias match {
      case ConstantType(Constant(sym: scala.Symbol)) =>
        sym.name

      case ConstantType(Constant(str: String)) =>
        str

      case t if t.baseType(symbolType) != NoType && t.baseClasses.contains(taggedType.asClass) =>
        val taggedTypeArg = t.baseType(taggedType) match {
          case TypeRef(_, _, args) if args.nonEmpty => args.head
          case _ => NoType
        }
        extractStringConstant(taggedTypeArg).getOrElse(litType.toString)

      case _ => litType.toString
    }
  }

  /** Given a complex labelled type, returns the label and the simple type. */
  def extractLabelAndTypeOption(tpe: fc.c.universe.Type): Option[(String, fc.c.universe.Type)] = {
    val dealiasedType = tpe.dealias

    dealiasedType match {
      case refinedType: RefinedType =>
        refinedType.parents.foldLeft[Option[(String, Type)]](None) {
          (acc, parent) =>
            if (acc.isDefined) acc
            else extractLabelAndTypeOption(parent)
        }

      case t if t.typeSymbol == fieldTypeSymbol || t.typeSymbol == keyTagSymbol =>
        val typeArgs = t.typeArgs
        if (typeArgs.size == 2) {
          val labelType = typeArgs(0)
          val valueType = typeArgs(1)
          val labelName = extractStringFromLiteral(labelType)

          Some((labelName, valueType))
        } else {
          None
        }

      case _ => None
    }
  }

  /** Given a complex labelled type, returns the label and the simple type, but aborts macro on failure. */
  def extractLabelAndType(tpe: fc.c.universe.Type): (String, fc.c.universe.Type) = {
    extractLabelAndTypeOption(tpe) match {
      case Some(labelAndType) => labelAndType
      case None =>
        fc.c.abort(fc.c.enclosingPosition, s"Failed to parse label or tag from: $tpe")
    }
  }

  /** Converts a possibly labelled HList of labelled types into a list of simple types and their labels. */
  def toLabeledTypeList(tpe: fc.c.universe.Type): List[fc.FieldType] = {
    toTypeList(tpe)
      .zipWithIndex.map { case (taggedTpe, idx) =>
      val (label, tpe) = extractLabelAndType(taggedTpe)
      fc.FieldType(tpe, label, idx)
    }
  }
}
