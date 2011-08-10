package bluemold.stm

import annotation.tailrec

/**
 * TransactionalHashTable<br/>
 * Author: Neil Essy<br/>
 * Created: 5/11/11<br/>
 * <p/>
 * [Description]
 */
object TransactionalFixedHashTable {
  /**
   * The maximum capacity, used if a higher value is implicitly specified
   * by either of the constructors with arguments.
   * MUST be a power of two <= 1<<30.
   */
  private[stm] val DEFAULT_CAPACITY: Int = 16
  private[stm] val MAXIMUM_CAPACITY: Int = 1 << 30
}
class TransactionalFixedHashTable[K,T]( requestedSize: Int ) {
  import Stm._
  val length = ensurePowerSize( requestedSize, TransactionalFixedHashTable.DEFAULT_CAPACITY )
  val table = initTable( new Array[Ref[Entry[K,T]]](length), 0 )
  val _size = new Ref[Int]( 0 )

  private def ensurePowerSize( requestedSize: Int, powerSize: Int ): Int = {
    if ( requestedSize <= powerSize ) powerSize
    else if ( requestedSize >= TransactionalFixedHashTable.MAXIMUM_CAPACITY ) TransactionalFixedHashTable.MAXIMUM_CAPACITY
    else ensurePowerSize( requestedSize, powerSize * 2 )
  }

  private def initTable( table: Array[Ref[Entry[K,T]]], index: Int ): Array[Ref[Entry[K,T]]] = {
    if ( index == size ) table
    else {
      table( index ) = new Ref[Entry[K,T]]( null )
      initTable( table, index + 1 )
    }
  }

  class Entry[K,T]( _key: K, initial: T ) {
    val key = _key 
    val next = new Ref[Entry[K,T]]( null )
    val value = new Ref[T]( initial )
  }

  def get( key: K ): Option[T] = {
    var ret: Option[T] = None
    if ( atomic {
      val entry = getEntry( key )
      if ( entry != null )
        ret = Some( entry.value.get() )
    } ) ret
    else get( key )
  }

  private def getEntry( key: K ): Entry[K,T] = {
    val index = indexFor( hashFun( key.hashCode ) )
    getEntry( key, table( index ) )
  }

  private def addEntry( entry: Entry[K,T] ) {
    val ref = table( indexFor( hashFun( entry.key.hashCode ) ) )
    val nextEntry = ref.get()
    entry.next.set( nextEntry )
    ref.set( entry )
  }

  @tailrec
  private def getEntry( key: K, ref: Ref[Entry[K,T]] ): Entry[K,T] = {
    val entry = ref.get()
    if ( entry == null ) null
    else if ( entry.key == key ) entry
    else getEntry( key, entry.next )
  }

  def put( key: K, value: T ) { put0( key, value ) }

  @tailrec
  private def put0( key: K, value: T ) {
    if ( ! atomic {
      val entry = getEntry( key )
      if ( entry == null )
        addEntry( new Entry[K,T]( key, value ) )
      else
        entry.value.set( value )
    } ) put0( key, value )
  }

  def insert( key: K, value: T ): Boolean = { false
  } 

  def remove( key: K ): Boolean = { false
    
  } 


  def update( key: K, value: T ): Boolean = { false
    
  }

  def conditionalUpdate( key: K, expect: T, update: T ): Boolean = { false
    
  }

  def size(): Int = _size.get()

  /**
   * Applies a supplemental hash function to a given hashCode, which
   * defends against poor quality hash functions.  This is critical
   * because HashMap uses power-of-two length hash tables, that
   * otherwise encounter collisions for hashCodes that do not differ
   * in lower bits. Note: Null keys always map to hash 0, thus index 0.
   */
  private def hashFun(_h: Int): Int = {
    val h = _h ^ ( ( _h >>> 20 ) ^ ( _h >>> 12 ) )
    h ^ ( h >>> 7 ) ^ ( h >>> 4 )
  }

  /**
   * Returns index for hash code h.
   */
  private def indexFor(h: Int): Int = h & ( length - 1 )
}