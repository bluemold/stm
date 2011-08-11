package bluemold.stm

import collection.mutable.HashMap
import bluemold.concurrent.casn.{CasnOp, TaggedValue, CasnSequence, CasnVar}
import bluemold.stm.Stm.Deferred
import annotation.tailrec

/**
 * StmObject<br/>
 * Author: Neil Essy<br/>
 * Created: 5/7/11<br/>
 * <p/>
 * [Description]
 */

final case class Binding( original: TaggedValue, current: Any )

final class Transaction {
  val refs = new HashMap[Ref[Any], Binding]
  var deferred: List[Deferred] = Nil
  var nesting = 0
  var aborted = false
  var commiting = false
  def commit(): Boolean = {
    commiting = true
    refs.toList match {
      case Nil => true
      case bindings => commit0( new CasnSequence(), bindings )
    }
  }
  def addDeferred( deferredOp: Deferred ) {
    deferred = deferredOp :: deferred
  }
  
  @tailrec
  private def commit0( seq: CasnSequence, bindings: List[(Ref[Any],Binding)] ): Boolean = {
    bindings match {
      case ( ref: Ref[Any], Binding( original, current ) ) :: tail => commit0( seq.casTaggedVal( ref.casnValue, original, current ), tail )
      case Nil => commit1( seq, deferred.reverse )
    }
  }

  @tailrec
  private def commit1( seq: CasnSequence, deferred: List[Deferred] ): Boolean = {
    deferred match {
      case head :: tail => commit1( head.addToSequence( seq ), tail )
      case Nil => seq.execute
    }
  }
}

final class Ref[+T]( initial: T ) {
  import Stm._
  val casnValue: CasnVar = CasnVar.create( initial )
  def dirtyGet(): T = casnValue.get.asInstanceOf[T]
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
      transaction.refs.get( this ) match {
        case None => {
          val taggedValue = if ( safer ) casnValue.safeGetTagged else casnValue.getTagged
          transaction.refs.put( this, Binding( taggedValue, taggedValue.value ) )
          taggedValue.value.asInstanceOf[T]
        }
        case Some( Binding( original, current ) ) => current.asInstanceOf[T]
      }
    } else casnValue.safeGet.asInstanceOf[T]
  } 
  def set[S]( update: S ) {
    set0( false, update )
  }
  def saferSet[S]( update: S ) {
    set0( true, update )
  }
  private def set0[S]( safer: Boolean, update: S ) {
    if ( hasTransaction ) {
      val transaction = getTransaction
      if ( transaction.commiting )
        throw new IllegalStateException( "Cannot perform additional atomic operations during the commit of a transaction" )
      transaction.refs.get( this ) match {
        case None => {
          val taggedValue = if ( safer ) casnValue.safeGetTagged else casnValue.getTagged
          transaction.refs.put( this, Binding( taggedValue, update ) )
        }
        case Some( Binding( original, current ) ) => transaction.refs.put( this, Binding( original, update ) )
      }
    } else casnValue.set( update )
  }
  def lock() {
    get()
  }
  def saferLock() {
    saferGet()
  }
  def expect[S]( expect: S ): Boolean = {
    get() == expect
  }
  def compareAndSet[S]( expect: S, update: S ): Boolean = {
    compareAndSet0( false, expect, update )
  }
  private def compareAndSet0[S]( safer: Boolean, expect: S, update: S ): Boolean = {
    if ( hasTransaction ) {
      val transaction = getTransaction
      if ( transaction.commiting )
        throw new IllegalStateException( "Cannot perform additional atomic operations during the commit of a transaction" )
      transaction.refs.get( this ) match {
        case None => {
          val taggedValue = if ( safer ) casnValue.safeGetTagged else casnValue.getTagged
          if ( expect == taggedValue.value ) {
            transaction.refs.put( this, Binding( taggedValue, update ) )
            true
          } else {
            transaction.refs.put( this, Binding( taggedValue, taggedValue.value ) )
            false
          }
        }
        case Some( Binding( original, current ) ) => {
          if ( expect == current ) {
            transaction.refs.put( this, Binding( original, update ) )
            true
          } else false
        }
      }
    } else casnValue.cas( expect, update )
  }
  def saferExpect[S]( expect: S ): Boolean = {
    saferGet() == expect
  }
}

object Stm {
  private val transactionLocal = new ThreadLocal[Transaction]
  def hasTransaction(): Boolean = transactionLocal.get != null
  def getTransaction(): Transaction = {
    val transaction = transactionLocal.get
    if ( transaction == null )
      throw new RuntimeException( "What Happened!" )
    transaction
  }
  private def startTransaction(): Transaction = {
    val transaction = transactionLocal.get
    if ( transaction == null ) {
      val newTransaction = new Transaction
      transactionLocal.set( newTransaction )
      newTransaction
    } else {
      transaction.nesting += 1
      transaction
    }
  }
  private def commitTransaction() {
    val transaction = transactionLocal.get
    if ( transaction == null ) {
      throw new RuntimeException( "What Happened!" )
    } else {
      if ( transaction.nesting == 0 ) {
        if ( ! transaction.aborted )
          transaction.aborted = ! transaction.commit
        transactionLocal.remove
      } else if ( transaction.nesting > 0 ) transaction.nesting -= 1
      else throw new RuntimeException( "What Happened!" )
    }
  }
  private def abortTransaction() {
    val transaction = transactionLocal.get
    if ( transaction == null ) {
      throw new RuntimeException( "What Happened!" )
    } else {
      transaction.aborted = true
    }
  }

  def atomic( body: => Unit ): Boolean = {
    val transaction = startTransaction
    try {
      body
    } catch {
      case t: Throwable => abortTransaction; throw new RuntimeException( "Exception caught inside atomic", t ) 
      case _ => abortTransaction; throw new RuntimeException( "Unknown Exception thrown inside atomic" ) 
    } finally {
      commitTransaction
    }
    ! transaction.aborted
  }

  def atomic[T]( body: => T ): Option[T] = {
    val transaction = startTransaction
    var res: Option[T] = None
    try {
      res = Some( body )
    } catch {
      case t: Throwable => abortTransaction; throw new RuntimeException( "Exception caught inside atomic", t )
      case _ => abortTransaction; throw new RuntimeException( "Unknown Exception thrown inside atomic" )
    } finally {
      commitTransaction
    }
    if ( ! transaction.aborted ) res else None
  }

  def deferredUpdateUsingSelf[T]( ref: Ref[T] )( compute: ( T ) => T ) = {
    val transaction = transactionLocal.get
    if ( transaction == null ) {
      throw new IllegalThreadStateException( "This can only be called inside a transaction" )
    } else
      transaction.addDeferred( new DeferredUpdateSelf( ref, compute ) )
  }
  def deferredUpdateUsing[T,S]( ref: Ref[T], source: Ref[S], compute: ( S ) => T ) = {
    val transaction = transactionLocal.get
    if ( transaction == null ) {
      throw new IllegalThreadStateException( "This can only be called inside a transaction" )
    } else
      transaction.addDeferred( new DeferredUpdateUsing( ref, source, compute ) )
  }
  def deferredExpect[T]( ref: Ref[T], compute: => T ) = {
    val transaction = transactionLocal.get
    if ( transaction == null ) {
      throw new IllegalThreadStateException( "This can only be called inside a transaction" )
    } else
      transaction.addDeferred( new DeferredExpect( ref, compute ) )
  }
  def deferredExpectUsing[T,S]( ref: Ref[T], source: Ref[S], compute: ( S ) => T ) = {
    val transaction = transactionLocal.get
    if ( transaction == null ) {
      throw new IllegalThreadStateException( "This can only be called inside a transaction" )
    } else
      transaction.addDeferred( new DeferredExpectUsing( ref, source, compute ) )
  }

  abstract class Deferred {
    def addToSequence( seq: CasnSequence ): CasnSequence
  }
  class DeferredUpdateSelf[T]( ref: Ref[T], compute: ( T ) => T ) extends Deferred {
    def addToSequence( seq: CasnSequence ) = {
      seq.set( ref.casnValue, ( op: CasnOp ) => compute( op.prior( 0 ).asInstanceOf[T] ) )
      seq
    }
  }
  class DeferredUpdateUsing[T,S]( ref: Ref[T], source: Ref[S], compute: ( S ) => T ) extends Deferred {
    def addToSequence( seq: CasnSequence ) = {
      seq.get( source.casnValue )
      seq.set( ref.casnValue, ( op: CasnOp ) => compute( op.prior( 1 ).asInstanceOf[S] ) )
      seq
    }
  }
  class DeferredExpect[T]( ref: Ref[T], compute: => T ) extends Deferred {
    def addToSequence( seq: CasnSequence ) = {
      seq.expect( ref.casnValue, ( op: CasnOp ) => compute )
      seq
    }
  }
  class DeferredExpectUsing[T,S]( ref: Ref[T], source: Ref[S], compute: ( S ) => T ) extends Deferred {
    def addToSequence( seq: CasnSequence ) = {
      seq.get( source.casnValue )
      seq.expect( ref.casnValue, ( op: CasnOp ) => compute( op.prior( 1 ).asInstanceOf[S] ) )
      seq
    }
  }
}


object StmTest {
  import Stm._
  
  def main( args: Array[String] ) {
    val refA = new Ref[Int]( 0 )
    val refB = new Ref[String]( null )
    val refC = new Ref[String]( null )
    
    refA.get // read commited
    refA.get // read commited
    refA.dirtyGet // read un-commited, never participates in a transaction. Is never considered as a "first read" or maintains repeatability.
    
    atomic { // read repeatable isolation, commit will fail if write was changed by another transaction from time it was first read inside transaction
      refA.set( 1 )
      refB.set( "Hello" )
    }

    println( "refA: " + refA.get ) 
    println( "refB: " + refB.get )
    println( "refC: " + refC.get )
  }
}