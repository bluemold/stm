package bluemold

import concurrent.casn.CasnSequence

package object stm {
	private val transactionLocal = new ThreadLocal[Transaction] {
    override def initialValue() = new Transaction
  }
  final def hasTransaction: Boolean = transactionLocal.get != null
	final def getTransaction: Transaction = transactionLocal.get
	private def startTransaction(): Transaction = {
	  val transaction = transactionLocal.get
	  if ( ! transaction.started ) {
      transaction.started = true
      transaction
	  } else {
  		transaction.nesting += 1
		transaction
	  }
	}
	private def commitTransaction( transaction: Transaction ) {
		if ( transaction.nesting == 0 ) {
		  if ( ! transaction.aborted )
			transaction.aborted = ! transaction.commit
		} else if ( transaction.nesting > 0 ) transaction.nesting -= 1
		else throw new RuntimeException( "What Happened!" )
	}
  private def commitTransactionWithGet[T]( transaction: Transaction, ref: Ref[T] ) = {
    var res: Option[T] = None
    if ( transaction.nesting == 0 ) {
      if ( ! transaction.aborted )
        res = transaction.commitWithGet( ref: Ref[T] )
      if ( res == None )
        transaction.aborted = true
      transactionLocal.remove()
    } else if ( transaction.nesting > 0 ) {
      res = Some( ref.get() )
      transaction.nesting -= 1
    } else throw new RuntimeException( "What Happened!" )
    res
  }
	private def abortTransaction() {
	  val transaction = transactionLocal.get
	  if ( !transaction.started ) {
		throw new RuntimeException( "What Happened!" )
	  } else {
		transaction.aborted = true
	  }
	}

  def atomic[T](body: => T): Option[T] = {
    val transaction = startTransaction()
    var res: Option[T] = None
    try {
      res = Some(body)
    } catch {
      case t: Throwable => abortTransaction(); throw new RuntimeException("Exception caught inside atomic", t)
    } finally {
      commitTransaction(transaction)
    }
    val aborted = transaction.aborted
    transaction.reset()
    if (!aborted) res else None
  }

  def atomicOn[S,T]( ref: Ref[T] )( body: => S ): Option[T] = {
    var res: Option[T] = None
    val transaction = startTransaction()
    try {
      body
    } catch {
      case t: Throwable => abortTransaction(); throw new RuntimeException( "Exception caught inside atomic", t )
    } finally {
      res = commitTransactionWithGet( transaction, ref )
      transaction.reset()
    }
    res
  }

  def atomicUpdate[T]( ref: Ref[T] )( fun: (T) => T ): T = {
    val transaction = transactionLocal.get
    if ( !transaction.started ) {
      val seq = CasnSequence
      .update( ref, fun )
      val success = seq.execute()
      if ( success )
        seq.getUpdateValue.value
      else
        throw new RuntimeException("!")
    } else { ref.set( fun( ref.get() ) ); ref.get() }
  }

	def deferredUpdate[T]( ref: Ref[T] )( compute: ( T ) => T ) {
	  val transaction = transactionLocal.get
	  if ( !transaction.started ) {
		throw new IllegalThreadStateException( "This can only be called inside a transaction" )
	  } else
		transaction.deferredUpdate( ref )( compute )
	}
	def deferredUpdateUsing[T,S]( ref: Ref[T] )( source: Ref[S] )( compute: ( T, S ) => T ) {
	  val transaction = transactionLocal.get
	  if ( !transaction.started ) {
		throw new IllegalThreadStateException( "This can only be called inside a transaction" )
	  } else
		transaction.deferredUpdateUsing( ref )( source )( compute )
	}
	def deferredExpect[T]( ref: Ref[T] )( compute: => T ) {
	  val transaction = transactionLocal.get
	  if ( !transaction.started ) {
		throw new IllegalThreadStateException( "This can only be called inside a transaction" )
	  } else
		transaction.deferredExpect( ref )( compute )
	}
	def deferredExpectUsing[T,S]( ref: Ref[T] )( source: Ref[S] )( compute: ( S ) => T ) {
	  val transaction = transactionLocal.get
	  if ( !transaction.started ) {
		throw new IllegalThreadStateException( "This can only be called inside a transaction" )
	  } else
		transaction.deferredExpectUsing( ref )( source )( compute )
	}
}