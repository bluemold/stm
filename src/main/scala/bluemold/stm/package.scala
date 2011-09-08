package bluemold

import concurrent.casn.CasnSequence

package object stm {
	private val transactionLocal = new ThreadLocal[Transaction]
	def hasTransaction: Boolean = transactionLocal.get != null
	def getTransaction: Transaction = {
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
		  transactionLocal.remove()
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

  def atomic[T](body: => T): Option[T] = {
    val transaction = startTransaction()
    var res: Option[T] = None
    try {
      res = Some(body)
    } catch {
      case t: Throwable => abortTransaction(); throw new RuntimeException("Exception caught inside atomic", t)
      case _ => abortTransaction(); throw new RuntimeException("Unknown Exception thrown inside atomic")
    } finally {
      commitTransaction()
    }
    if (!transaction.aborted) res else None
  }

  def atomicDeferred( body: => Unit ): Option[Any] = {
    val transaction = startTransaction()
    try {
      body
    } catch {
      case t: Throwable => abortTransaction(); throw new RuntimeException( "Exception caught inside atomic", t )
      case _ => abortTransaction(); throw new RuntimeException( "Unknown Exception thrown inside atomic" )
    } finally {
      commitTransaction()
    }
    transaction.getLastUpdate
  }

  def atomicOn[T]( ref: Ref[T] )( fun: (T) => T ): Option[T] = {
    val transaction = transactionLocal.get
    if ( transaction == null ) {
      val seq = new CasnSequence()
      seq.set( ref.casnValue, (op) => fun( op.prior( 0 ).asInstanceOf[T] ) )
      seq.executeOption().asInstanceOf[Option[T]]
    } else ( atomicDeferred { deferredUpdateUsingSelf( ref )( fun ) } ).asInstanceOf[Option[T]]
  }

	def deferredUpdateUsingSelf[T]( ref: Ref[T] )( compute: ( T ) => T ) {
	  val transaction = transactionLocal.get
	  if ( transaction == null ) {
		throw new IllegalThreadStateException( "This can only be called inside a transaction" )
	  } else
		transaction.addDeferred( new DeferredUpdateSelf( ref, compute ) )
	}
	def deferredUpdateUsing[T,S]( ref: Ref[T] )( source: Ref[S] )( compute: ( S ) => T ) {
	  val transaction = transactionLocal.get
	  if ( transaction == null ) {
		throw new IllegalThreadStateException( "This can only be called inside a transaction" )
	  } else
		transaction.addDeferred( new DeferredUpdateUsing( ref, source, compute ) )
	}
	def deferredExpect[T]( ref: Ref[T] )( compute: => T ) {
	  val transaction = transactionLocal.get
	  if ( transaction == null ) {
		throw new IllegalThreadStateException( "This can only be called inside a transaction" )
	  } else
		transaction.addDeferred( new DeferredExpect( ref, compute ) )
	}
	def deferredExpectUsing[T,S]( ref: Ref[T] )( source: Ref[S] )( compute: ( S ) => T ) {
	  val transaction = transactionLocal.get
	  if ( transaction == null ) {
		throw new IllegalThreadStateException( "This can only be called inside a transaction" )
	  } else
		transaction.addDeferred( new DeferredExpectUsing( ref, source, compute ) )
	}
}