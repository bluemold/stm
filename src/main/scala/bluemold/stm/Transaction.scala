package bluemold.stm

import scala.language.existentials
import annotation.tailrec
import collection.mutable.HashMap
import bluemold.concurrent.casn._

final case class Binding[T]( ref: Ref[T], original: TaggedValue[T], current: T )

final class Transaction {
  var _refs: HashMap[Ref[_], Binding[_]] = new HashMap[Ref[_], Binding[_]]()
  private var deferred: List[Deferred[_]] = Nil
  var nesting: Int = _
  var aborted: Boolean = _
  var commiting: Boolean = _
  var started: Boolean = _
  def reset() {
    started = false
    aborted = false
    commiting = false
    _refs.clear()
    deferred = Nil
  }
  @inline
  private[stm] def getBinding[T]( target: Ref[T] ): Option[Binding[T]] =
    _refs.get( target ).asInstanceOf[Option[Binding[T]]]

  @inline
  @tailrec
  private[stm] def getListBinding[T]( target: Ref[T], refs: List[Binding[_]] ): Option[Binding[T]] =
    if ( refs eq Nil ) None
    else {
      val binding :: tail = refs
      if ( binding.ref == target ) Some( binding.asInstanceOf[Binding[T]] )
      else getListBinding( target, tail )
    }

  @inline
  private[stm] def setBinding( binding: Binding[_] ) {
    _refs.put( binding.ref, binding )
  }

  @inline
  private[stm] def updateBinding( binding: Binding[_] ) {
    _refs.put( binding.ref, binding )
  }

  @inline
  def commit(): Boolean = {
    commiting = true
    if ( _refs == null )
        if ( deferred.isEmpty )
          true
        else
          addDeferred( NoOp, deferred ).execute()
    else {
        val bindingOps = createBindingOps( _refs.values )
        if ( deferred.isEmpty )
          bindingOps.execute()
        else
          addDeferred( bindingOps, deferred ).execute()
    }
  }

  @inline
  def commitWithGet[T]( ref: Ref[T] ): Option[T] = {
    commiting = true
    if ( _refs == null )
        if ( deferred.isEmpty )
          NoOp.get( ref ).executeOption()
        else
          addDeferred( NoOp, deferred ).get( ref ).executeOption()
    else {
        val bindingOps = createBindingOps( _refs.values )
        if ( deferred.isEmpty )
          bindingOps.get( ref ).executeOption()
        else
          addDeferred( bindingOps, deferred ).get( ref ).executeOption()
    }
  }

  @inline
  private def createBindingOps( bindings: Iterable[Binding[_]] ): CasnOp[_] = {
    var op: CasnOp[_] = NoOp
    val it = bindings.iterator
    while ( it.hasNext ) {
      val binding = it.next()
      op = op.casTaggedVal( binding.ref.asInstanceOf[Ref[Any]],
        binding.original.asInstanceOf[TaggedValue[Any]],
        binding.current )
    }
    op
  }

  @inline
  @tailrec
  private def addDeferred( seq: CasnOp[_], deferred: List[Deferred[_]] ): CasnOp[_] =
    if ( deferred eq Nil ) seq
    else {
      val op :: tail = deferred 
      addDeferred( op.addToSequence( seq ), tail )
    }

  def deferredUpdate[T]( ref: Ref[T] )( compute: ( T ) => T ) { 
    deferred ::= new DeferredUpdateSelf[T]( ref, compute )
  }
  def deferredUpdateUsing[T,S]( ref: Ref[T] )( source: Ref[S] )( compute: ( T, S ) => T ) {
    deferred ::= new DeferredUpdateUsing[T,S]( ref, source, compute )
  }
  def deferredExpect[T]( ref: Ref[T] )( compute: => T ) {
    deferred ::= new DeferredExpect[T]( ref, compute )
  }
  def deferredExpectUsing[T,S]( ref: Ref[T] )( source: Ref[S] )( compute: ( S ) => T ) {
    deferred ::= new DeferredExpectUsing[T,S]( ref, source, compute )
  }
  
  private abstract sealed class Deferred[T] {
    def addToSequence( seq: CasnOp[_] ): CasnOp[T]
  }
  private final class DeferredUpdateSelf[T]( ref: Ref[T], compute: ( T ) => T ) extends Deferred[T] {
    def addToSequence( seq: CasnOp[_] ) =
      seq.update( ref, compute )
  }
  private final class DeferredUpdateUsing[T,S]( ref: Ref[T], source: Ref[S], compute: ( T, S ) => T ) extends Deferred[T] {
    def addToSequence( seq: CasnOp[_] ) =
      seq.get( source ).set( ref, ( op: CasnOp[T] ) => compute( op.prior( 0 ).asInstanceOf[T], op.prior( 1 ).asInstanceOf[S] ) )
  }
  private final class DeferredExpect[T]( ref: Ref[T], compute: => T ) extends Deferred[T] {
    def addToSequence( seq: CasnOp[_] ) =
      seq.expect( ref, ( op: CasnOp[T] ) => compute )
  }
  private final class DeferredExpectUsing[T,S]( ref: Ref[T], source: Ref[S], compute: ( S ) => T ) extends Deferred[T] {
    def addToSequence( seq: CasnOp[_] ) =
      seq.get( source ).expect( ref, ( op: CasnOp[T] ) => compute( op.prior( 1 ).asInstanceOf[S] ) )
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
    val transaction = getTransaction
    if ( transaction.started ) {
      if ( transaction.commiting )
        throw new IllegalStateException( "Cannot perform additional atomic operations during the commit of a transaction" )
      val binding = transaction.getBinding( this )
      if ( binding eq None ) {
          val taggedValue = if ( safer ) safeGetTagged() else getTagged
          transaction.setBinding( Binding( this, taggedValue, taggedValue.value ) )
          taggedValue.value
      } else {
        val Some( Binding( _, _, current ) ) = binding
        current
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
    val transaction = getTransaction
    if ( transaction.started ) {
      if ( transaction.commiting )
        throw new IllegalStateException( "Cannot perform additional atomic operations during the commit of a transaction" )
      val binding = transaction.getBinding( this )
      if ( binding eq None ) {
        val taggedValue = if ( safer ) safeGetTagged() else getTagged
        transaction.setBinding( Binding( this, taggedValue, update ) )
      } else {
        val Some( Binding( _, original, current ) ) = binding
        transaction.updateBinding( Binding( this, original, update ) )
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
    val transaction = getTransaction
    if ( transaction.started ) {
      if ( transaction.commiting )
        throw new IllegalStateException( "Cannot perform additional atomic operations during the commit of a transaction" )
      val binding = transaction.getBinding( this )
      if ( binding eq None ) {
          val taggedValue = if ( safer ) safeGetTagged() else getTagged
          if ( expect == taggedValue.value ) {
            transaction.setBinding( Binding( this, taggedValue, update ) )
            true
          } else {
            transaction.setBinding( Binding( this, taggedValue, taggedValue.value ) )
            false
          }
      } else {
        val Some( Binding( _, original, current ) ) = binding
        if ( expect == current ) {
          transaction.updateBinding( Binding( this, original, update ) )
          true
        } else false
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
