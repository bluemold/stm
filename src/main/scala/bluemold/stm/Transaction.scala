package bluemold.stm

import annotation.tailrec
import collection.mutable.HashMap
import org.bluemold.unsafe.Unsafe
import java.util.concurrent.{Semaphore, ConcurrentLinkedQueue}

final class Binding[T]( _ref: Ref[T], _original: RefValue[T] ) {
  def ref = _ref
  def original = _original
}

class TransactionFailed extends RuntimeException

private[stm] object Transaction {
  import Unsafe._
  val waitingOnIndex = objectDeclaredFieldOffset( classOf[Transaction], "waitingOn" )

  implicit def currentTransaction: Transaction = getTransaction
}

final class Transaction {
  import Transaction._
  import Unsafe._

  @volatile var waitingOn: Ref[_] = _
  val semaphore = new Semaphore( 0 )

  var bindings: HashMap[Ref[_], Binding[_]] = _
  var writing: List[Ref[_]] = Nil

  var nesting: Int = _
  var aborted: Boolean = _

  private[stm] def reset() {
    aborted = false
    writing = Nil
    if ( bindings != null )
      bindings.clear()
  }
  @inline
  private[stm] def updateWaitingOn( expect: Ref[_], update: Ref[_] ) =
    compareAndSwapObject( this, waitingOnIndex, expect, update )
  
  @inline
  private[stm] def getBinding[T]( target: Ref[T] ): Option[Binding[T]] = {
    if ( bindings == null ) None
    else bindings.get( target ).asInstanceOf[Option[Binding[T]]]
  }

  @inline
  private[stm] def setBinding( binding: Binding[_] ) {
    if ( bindings == null )
      bindings = new HashMap[Ref[_], Binding[_]]()
    bindings.put( binding.ref, binding )
  }

  @inline
  private[stm] def updateBinding( binding: Binding[_] ) {
    bindings.update( binding.ref, binding )
  }

  @inline
  private[stm] def removeBinding( binding: Binding[_] ) {
    bindings.remove( binding.ref )
  }

  @inline
  private def getRef0[T]( ref: Ref[T] ): T = {
    val refValue = ref.value
    val lockedBy = refValue.lockedBy
    if ( refValue.lockedBy == null ) refValue.value
    else if ( refValue.lockedBy eq this ) refValue.value
    else {
      ref.waitForUnlock( this, lockedBy )
      getRef0( ref )
    }
  }

  @inline
  private def revert0() {
    var refs = writing
    while ( refs ne Nil ) {
      val ref = refs.head
      ref.revert()
      ref.queue.peek() match {
        case t: Transaction => t.semaphore.release()
        case null => // no other transactions to wake up
      }
      refs = refs.tail
    }
  }

  @inline
  private def unlock0() {
    var refs = writing
    while ( refs ne Nil ) {
      val ref = refs.head
      ref.unlock()
      ref.queue.peek() match {
        case t: Transaction => t.semaphore.release()
        case null => // no other transactions to wake up
      }
      refs = refs.tail
    }
  }

  @inline
  def commit(): Boolean = {
    if ( ! aborted ) {
      if ( bindings != null ) {
        var values = bindings.values
        while ( ! aborted && values.isEmpty ) {
          val binding = values.head
          if ( binding.ref.value ne binding.original ) aborted = false
          else values = values.tail
        }
      }
      
      if ( ! aborted ) {
        unlock0()
        true
      } else {
        revert0()
        false
      }
    } else {
      revert0()
      false
    }
  }

  @inline
  def commitWithGet[T]( ref: Ref[T] ): Option[T] = {
    try {
      if ( ! aborted ) {
        val value = getBinding( ref ) match {
          case None => getRef0( ref )
          case Some( binding ) => binding.original.value
        }
  
        if ( bindings != null ) {
          var values = bindings.values
          while ( ! aborted && values.isEmpty ) {
            val binding = values.head
            if ( binding.ref.value ne binding.original ) aborted = false
            else values = values.tail
          }
        }
      
        if ( ! aborted ) {
          unlock0()
          Some( value )
        } else {
          revert0()
          None
        }
      } else {
        revert0()
        None
      }
    } catch {
      case t: TransactionFailed => 
        revert0()
        None
    }
  }
}

final class RefValue[T]( _value: T, _lockedBy: Transaction, _prior: RefValue[T] ) {
  def value = _value
  def lockedBy = _lockedBy
  def prior = _prior
  def copyUnlocked() = new RefValue( _value, null, null )
}

object Ref {
  import Unsafe._
  def apply[T]( initial: T ) = new Ref( initial )
  private[stm] val valueIndex = objectDeclaredFieldOffset( classOf[Ref[_]], "value" )
}

final class Ref[T]( initial: T ) {
  import Unsafe._
  import Ref._
  val queue = new ConcurrentLinkedQueue[Transaction]
  @volatile var value: RefValue[T] = new RefValue[T]( initial, null, null )

  @inline def updateValue( expect: RefValue[T], update: RefValue[T] ) = compareAndSwapObject( this, valueIndex, expect, update ) 
  @inline def dirtyGet(): T = value.value

  @inline
  private[stm] def unlock() {
    value = value.copyUnlocked()
  } 
  
  @inline
  private[stm] def revert() {
    value = value.prior
  } 

  @inline
  @tailrec
  private[stm] def checkForDeadLock( transaction: Transaction, lockedBy: Transaction ): Boolean = {
    val waitingOn = lockedBy.waitingOn
    if ( waitingOn == null ) false
    else {
      val nextRefValue: RefValue[_] = waitingOn.value
      val nextLockedBy = nextRefValue.lockedBy
      if ( nextLockedBy eq transaction ) true
      else checkForDeadLock( transaction, nextLockedBy )
    }
  }

    
  @inline
  private[stm] def waitForUnlock( transaction: Transaction, lockedBy: Transaction ) {
    transaction.waitingOn = this
    if ( checkForDeadLock( transaction, lockedBy ) ) {
      transaction.waitingOn = null
      throw new TransactionFailed
    }
    
    val semaphore = transaction.semaphore
    semaphore.drainPermits()
    while ( ! queue.add( transaction ) ) {} // loop until successful
    if ( queue.peek() eq transaction ) {
      // double check lock
      val refValue = value
      if ( refValue.lockedBy != null ) {
        try {
          semaphore.acquire()
        } catch {
          case t: InterruptedException => // do nothing
        }
        if ( queue.poll() ne transaction ) // pop self off queue
          throw new RuntimeException( "What Happened!" )
      } else {
        if ( queue.poll() ne transaction ) // pop self off queue
          throw new RuntimeException( "What Happened!" )
      } 
    } else {
      try {
        semaphore.acquire()
      } catch {
        case t: InterruptedException => // do nothing
      }
      if ( queue.poll() ne transaction ) // pop self off queue
        throw new RuntimeException( "What Happened!" )
    }
    // now try again
    transaction.waitingOn = null
  }

  @inline
  @tailrec
  private def get0( transaction: Transaction ): T = {
    val refValue = value
    val lockedBy = refValue.lockedBy
    if ( lockedBy == null ) {
      transaction.setBinding( new Binding( this, refValue ) )
      refValue.value
    } else if ( lockedBy eq transaction ) {
      refValue.value
    } else {
      waitForUnlock( transaction, lockedBy )
      get0( transaction )
    }
  }

  @inline
  @tailrec
  private def getSingle0( transaction: Transaction ): T = {
    val refValue = value
    val lockedBy = refValue.lockedBy
    if ( lockedBy == null ) {
      refValue.value
    } else if ( lockedBy eq transaction ) {
      refValue.value // Todo, ???
    } else {
      waitForUnlock( transaction, lockedBy )
      getSingle0( transaction )
    }
  }

  def get()( implicit transaction: Transaction ): T = {
    if ( transaction != null ) {
      transaction.getBinding( this ) match {
        case None => get0( transaction )
        case Some( binding ) => binding.original.value
      }
    } else {
      val refValue = value
      val lockedBy = refValue.lockedBy
      if ( lockedBy == null ) refValue.value
      else {
        val transaction = new Transaction
        waitForUnlock( transaction, lockedBy )
        getSingle0( transaction )
      }
    }
  } 

  @inline
  @tailrec
  private def set0( transaction: Transaction, update: T ) {
    val refValue = value
    val lockedBy = refValue.lockedBy
    if ( lockedBy == null ) {
      val newRefValue = new RefValue[T]( update, transaction, refValue )
      if ( updateValue( refValue, newRefValue ) ) transaction.writing ::= this
      else set0( transaction, update ) // try again 
    } else if ( refValue.lockedBy eq transaction ) {
      if ( ! updateValue( refValue, new RefValue( update, transaction, refValue.prior ) ) )
        throw new RuntimeException( "What Happened!" )
    } else {
      waitForUnlock( transaction, lockedBy )
      set0( transaction, update )
    }
  }

  @inline
  @tailrec
  private def setSingle0( transaction: Transaction, update: T ) {
    val refValue = value
    val lockedBy = refValue.lockedBy
    if ( lockedBy == null ) {
      val newRefValue = new RefValue[T]( update, null, null )
      if ( ! updateValue( refValue, newRefValue ) )
        setSingle0( transaction, update ) // try again
    } else {
      waitForUnlock( transaction, lockedBy )
      setSingle0( transaction, update ) // try again
    }
  }

  def set( update: T )( implicit transaction: Transaction ) {
    if ( transaction != null ) {
      transaction.getBinding( this ) match {
        case None => set0( transaction, update )
        case Some( binding ) => {
          val refValue = value
          if ( binding.original eq refValue ) {
            val newRefValue = new RefValue[T]( update, transaction, refValue )
            if ( updateValue( refValue, newRefValue ) ) {
              transaction.writing ::= this
              transaction.removeBinding( binding )
            } else throw new TransactionFailed
          } else throw new TransactionFailed
        }
      }
    } else {
      val refValue = value
      val lockedBy = refValue.lockedBy
      if ( lockedBy == null ) {
        val newRefValue = new RefValue[T]( update, null, null )
        if ( ! updateValue( refValue, newRefValue ) )
          setSingle0( new Transaction(), update ) // try again
      } else {
        val transaction = new Transaction()
        waitForUnlock( transaction, lockedBy )
        setSingle0( transaction, update ) // try again
      }
    }
  }

  @inline
  @tailrec
  private def lock0( transaction: Transaction ) {
    val refValue = value
    val lockedBy = refValue.lockedBy
    if ( lockedBy == null ) {
      val newRefValue = new RefValue[T]( refValue.value, transaction, refValue )
      if ( updateValue( refValue, newRefValue ) ) transaction.writing ::= this
      else lock0( transaction ) // try again 
    } else if ( lockedBy ne transaction ) {
      waitForUnlock( transaction, lockedBy )
      lock0( transaction )
    }
  }

  def lock()( implicit transaction: Transaction ) {
    if ( transaction != null ) {
      transaction.getBinding( this ) match {
        case None => lock0( transaction )
        case Some( binding ) => {
          val refValue = value
          if ( binding.original eq refValue ) {
            val newRefValue = new RefValue[T]( refValue.value, transaction, refValue )
            if ( updateValue( refValue, newRefValue ) ) {
              transaction.writing ::= this
              transaction.removeBinding( binding )
            } else throw new TransactionFailed
          } else throw new TransactionFailed
        }
      }
    } else throw new RuntimeException( "Not inside a transaction" )
  }

  def expect( expect: T )( implicit transaction: Transaction ): Boolean = get()( transaction ) == expect

  @inline
  @tailrec
  private def alter0( transaction: Transaction, update: (T) => T ): T = {
    val refValue = value
    val lockedBy = refValue.lockedBy
    if ( lockedBy == null ) {
      val newValue = update( refValue.value )
      val newRefValue = new RefValue[T]( newValue, transaction, refValue )
      if ( updateValue( refValue, newRefValue ) ) {
        transaction.writing ::= this
        newValue
      }
      else alter0( transaction, update ) // try again 
    } else if ( lockedBy eq transaction ) {
      val newValue = update( refValue.value )
      if ( ! updateValue( refValue, new RefValue( newValue, transaction, refValue.prior ) ) )
        throw new RuntimeException( "What Happened!" )
      newValue
    } else {
      waitForUnlock( transaction, lockedBy )
      alter0( transaction, update )
    }
  }

  @inline
  @tailrec
  private def alterSingle0( transaction: Transaction, update: (T) => T ): T = {
    val refValue = value
    val lockedBy = refValue.lockedBy
    if ( lockedBy == null ) {
      val newValue = update( refValue.value )
      val newRefValue = new RefValue[T]( newValue, null, null )
      if ( updateValue( refValue, newRefValue ) ) newValue
      else alterSingle0( transaction, update ) // try again
    } else {
      waitForUnlock( transaction, lockedBy )
      alterSingle0( transaction, update ) // try again
    }
  }


  def alter( update: (T) => T )( implicit transaction: Transaction ): T = {
    if ( transaction != null ) {
      transaction.getBinding( this ) match {
        case None => alter0( transaction, update )
        case Some( binding ) => {
          val refValue = value
          if ( binding.original eq refValue ) {
            val newValue = update( refValue.value )
            val newRefValue = new RefValue[T]( newValue, transaction, refValue )
            if ( updateValue( refValue, newRefValue ) ) {
              transaction.removeBinding( binding )
              newValue
            } else throw new TransactionFailed
          } else throw new TransactionFailed
        }
      }
    } else {
      val refValue = value
      val lockedBy = refValue.lockedBy
      if ( lockedBy == null ) {
        val newValue = update( refValue.value )
        val newRefValue = new RefValue[T]( newValue, null, null )
        if ( updateValue( refValue, newRefValue ) ) newValue
        else alterSingle0( new Transaction(), update ) // try again
      } else {
        val transaction = new Transaction()
        waitForUnlock( transaction, lockedBy )
        alterSingle0( transaction, update ) // try again
      }
    }
  }
}
