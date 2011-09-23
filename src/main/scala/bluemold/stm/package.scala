package bluemold

package object stm {
  private val transactionLocal = new ThreadLocal[Transaction]

  def getTransaction: Transaction = transactionLocal.get

  private def startTransaction(): Transaction = {
    val transaction = transactionLocal.get
    if (transaction == null) {
      val newTransaction = new Transaction
      transactionLocal.set(newTransaction)
      newTransaction
    } else {
      transaction.nesting += 1
      transaction
    }
  }

  private def commitTransaction( transaction: Transaction ) {
    if (transaction.nesting == 0) {
      if (!transaction.aborted)
        transaction.aborted = !transaction.commit
    }
    transaction.nesting -= 1
  }

  private def commitTransactionWithGet[T]( transaction: Transaction, ref: Ref[T] ) = {
    var res: Option[T] = None
    if ( transaction.nesting == 0 ) {
      if ( ! transaction.aborted )
        res = transaction.commitWithGet( ref: Ref[T] )
      if ( res == None )
        transaction.aborted = true
    } else if ( transaction.nesting > 0 ) {
      res = Some( ref.get()( transaction ) )
    } 
    transaction.nesting -= 1
    res
  }

  def atomic[T](body: => T): Option[T] = {
    val transaction = startTransaction()
    var res: Option[T] = None
    try {
      res = Some(body)
    } catch {
      case t: TransactionFailed => transaction.aborted = true
      case t: Throwable => transaction.aborted = true; throw new RuntimeException("Exception caught inside atomic", t)
      case _ => transaction.aborted = true; throw new RuntimeException("Unknown Exception thrown inside atomic")
    } finally {
      commitTransaction( transaction )
    }
    if (!transaction.aborted) {
      if ( transaction.nesting == -1 )
        transaction.reset()
      res
    } else {
      if ( transaction.nesting == -1 )
        transaction.reset()
      None
    }
  }

  def atomicOn[S,T]( ref: Ref[T] )( body: => S ): Option[T] = {
    var res: Option[T] = None
    val transaction = startTransaction()
    try {
      body
    } catch {
      case t: TransactionFailed => transaction.aborted = true
      case t: Throwable => transaction.aborted = true; throw new RuntimeException( "Exception caught inside atomic", t )
      case _ => transaction.aborted = true; throw new RuntimeException( "Unknown Exception thrown inside atomic" )
    } finally {
      res = commitTransactionWithGet( transaction, ref )
    }
    if ( transaction.nesting == -1 )
      transaction.reset()
    res
  }
}