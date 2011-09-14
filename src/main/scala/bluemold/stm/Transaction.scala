package bluemold.stm

import annotation.tailrec
import collection.mutable.HashMap
import bluemold.concurrent.casn._

final case class Binding[T]( ref: Ref[T], original: TaggedValue[T], current: T )

private[stm] object Transaction {
  val maxRefList = 8
}
final class Transaction {
  import Transaction._
  var _refs: HashMap[Ref[_], Binding[_]] = null
  var firstDeferred: CasnOp[_] = null
  var deferred: CasnOp[_] = null
  var nesting = 0
  var aborted = false
  var commiting = false

  @inline
  private[stm] def getBinding[T]( target: Ref[T] ): Option[Binding[T]] = {
    _refs match {
      case null => None
      case bindings => bindings.get( target ).asInstanceOf[Option[Binding[T]]]
    }
  }
  @inline
  @tailrec
  private[stm] def getListBinding[T]( target: Ref[T], refs: List[Binding[_]] ): Option[Binding[T]] = refs match {
    case Nil => None
    case binding :: tail =>
      if ( binding.ref == target ) Some( binding.asInstanceOf[Binding[T]] )
      else getListBinding( target, tail )
  }

  @inline
  private[stm] def setBinding( binding: Binding[_] ) {
    if ( _refs == null )
      _refs = new HashMap[Ref[_], Binding[_]]
    _refs.put( binding.ref, binding )
  }

  @inline
  private[stm] def updateBinding( binding: Binding[_] ) {
    if ( _refs == null )
      _refs = new HashMap[Ref[_], Binding[_]]
    _refs.put( binding.ref, binding )
  }

  @inline
  def commit(): Boolean = {
    commiting = true
    _refs match {
      case null =>
        if ( deferred  == null ) true
        else new CasnSequence[Any]( deferred.asInstanceOf[CasnOp[Any]] ).execute()
      case bindings =>
        val bindingOps = createBindingOps( bindings.values )
        if ( deferred == null ) new CasnSequence[Any]( bindingOps.asInstanceOf[CasnOp[Any]] ).execute()
        else {
          bindingOps.nextOp = firstDeferred
          firstDeferred.prevOp = bindingOps
          new CasnSequence[Any]( deferred.asInstanceOf[CasnOp[Any]] ).execute()
        }
    }
  }

  @inline
  def commitWithGet[T]( ref: Ref[T] ): Option[T] = {
    commiting = true
    _refs match {
      case null =>
        if ( deferred == null ) new CasnSequence[T]( NoOp.get( ref ) ).executeOption()
        else new CasnSequence[T]( deferred.get( ref ) ).executeOption()
      case bindings =>
        val bindingOps = createBindingOps( bindings.values )
        if ( deferred == null ) new CasnSequence[T]( bindingOps.get( ref ) ).executeOption()
        else {
          bindingOps.nextOp = firstDeferred
          firstDeferred.prevOp = bindingOps
          new CasnSequence[T]( deferred.get( ref ) ).executeOption()
        }
    }
  }

  @inline
  private def createBindingOps( bindings: Iterable[Binding[_]] ): CasnOp[_] = {
    var op: CasnOp[_] = NoOp
    bindings foreach { binding =>
      op = op.casTaggedVal( binding.ref.asInstanceOf[Ref[Any]],
        binding.original.asInstanceOf[TaggedValue[Any]],
        binding.current.asInstanceOf[Any] )
    }
    op
  }

  def deferredUpdate[T]( ref: Ref[T] )( compute: ( T ) => T ) {
    if ( firstDeferred == null ) {
      firstDeferred = NoOp.update( ref, compute )
      deferred = firstDeferred
    } else deferred = deferred.update( ref, compute )
  }
  def deferredUpdateUsing[T,S]( ref: Ref[T] )( source: Ref[S] )( compute: ( T, S ) => T ) {
    if ( firstDeferred == null ) {
      val getOp: CasnOp[S] = NoOp.get( source )
      firstDeferred = getOp
      deferred = getOp.update( ref , (old: T) => compute( old, getOp.getPrevValue.value ) )
    } else {
      val getOp: CasnOp[S] = deferred.get( source )
      deferred = getOp.update( ref , (old: T) => compute( old, getOp.getPrevValue.value ) )
    }
  }
  def deferredExpect[T]( ref: Ref[T] )( compute: => T ) {
    if ( firstDeferred == null ) {
      firstDeferred = NoOp.expect( ref, (op: CasnOp[T] ) => compute )
      deferred = firstDeferred
    } else deferred = deferred.expect( ref, (op: CasnOp[T] ) => compute )
  }
  def deferredExpectUsing[T,S]( ref: Ref[T] )( source: Ref[S] )( compute: ( S ) => T ) {
    if ( firstDeferred == null ) {
      val getOp: CasnOp[S] = NoOp.get( source )
      firstDeferred = getOp
      deferred = getOp.expect( ref , (op: CasnOp[T]) => compute( getOp.getPrevValue.value ) )
    } else {
      val getOp: CasnOp[S] = deferred.get( source )
      deferred = getOp.expect( ref , (op: CasnOp[T]) => compute( getOp.getPrevValue.value ) )
    }
  }
  
}
object Ref {
  def apply[T]( initial: T ) = new Ref( initial )
}
final class Ref[T]( initial: T ) extends CasnVar[T]( initial ) {
  def dirtyGet(): T = getValue
  def get(): T = {
    get0( false )
  }
  def saferGet(): T = {
    get0( true )
  }
  private def get0( safer: Boolean ): T = {
    if ( hasTransaction ) {
      val transaction = getTransaction
      if ( transaction.commiting )
        throw new IllegalStateException( "Cannot perform additional atomic operations during the commit of a transaction" )
      transaction.getBinding( this ) match {
        case None => {
          val taggedValue = if ( safer ) safeGetTagged() else getTagged
          transaction.setBinding( Binding( this, taggedValue, taggedValue.value ) )
          taggedValue.value
        }
        case Some( Binding( _, original, current ) ) => current.asInstanceOf[T]
      }
    } else safeGet()
  } 
  def set( update: T ) {
    set0( false, update )
  }
  def saferSet( update: T ) {
    set0( true, update )
  }
  private def set0( safer: Boolean, update: T ) {
    if ( hasTransaction ) {
      val transaction = getTransaction
      if ( transaction.commiting )
        throw new IllegalStateException( "Cannot perform additional atomic operations during the commit of a transaction" )
      transaction.getBinding( this ) match {
        case None => {
          val taggedValue = if ( safer ) safeGetTagged() else getTagged
          transaction.setBinding( Binding( this, taggedValue, update ) )
        }
        case Some( Binding( _, original, current ) ) => transaction.updateBinding( Binding( this, original, update ) )
      }
    } else safeSet( update )
  }
  def lock() {
    get()
  }
  def saferLock() {
    saferGet()
  }
  def expect( expect: T ): Boolean = {
    get() == expect
  }
  def compareAndSet( expect: T, update: T ): Boolean = {
    compareAndSet0( false, expect, update )
  }
  private def compareAndSet0( safer: Boolean, expect: T, update: T ): Boolean = {
    if ( hasTransaction ) {
      val transaction = getTransaction
      if ( transaction.commiting )
        throw new IllegalStateException( "Cannot perform additional atomic operations during the commit of a transaction" )
      transaction.getBinding( this ) match {
        case None => {
          val taggedValue = if ( safer ) safeGetTagged() else getTagged
          if ( expect == taggedValue.value ) {
            transaction.setBinding( Binding( this, taggedValue, update ) )
            true
          } else {
            transaction.setBinding( Binding( this, taggedValue, taggedValue.value ) )
            false
          }
        }
        case Some( Binding( _, original, current ) ) => {
          if ( expect == current ) {
            transaction.updateBinding( Binding( this, original, update ) )
            true
          } else false
        }
      }
    } else safeCas( expect, update )
  }
  def saferExpect( expect: T ): Boolean = {
    saferGet() == expect
  }
  def alter( update: (T) => T ): T = {
    set( update( get() ) )
    get()
  }
}

object StmTest {
  def main( args: Array[String] ) {
    val refA = new Ref[Int]( 0 )
    val refB = new Ref[String]( null )
    val refC = new Ref[String]( null )
    
    refA.get() // read commited
    refA.get() // read commited
    refA.dirtyGet() // read un-commited, never participates in a transaction. Is never considered as a "first read" or maintains repeatability.
    
    atomic { // read repeatable isolation, commit will fail if write was changed by another transaction from time it was first read inside transaction
      refA.set( 1 )
      refB.set( "Hello" )
    }

    println( "refA: " + refA.get ) 
    println( "refB: " + refB.get )
    println( "refC: " + refC.get )
  }
}