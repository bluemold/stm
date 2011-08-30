package test.bluemold.stm

import junit.framework._
import Assert._
import annotation.tailrec
import bluemold.stm._
import bluemold.stm.TransactionalFixedHashTable

object StmTest {
    def suite: Test = {
        val suite = new TestSuite(classOf[StmTest])
        suite
    }

    def main(args : Array[String]) {
        junit.textui.TestRunner.run(suite)
    }
}

/**
 * Unit tests for stm.
 */
class StmTest extends TestCase("stm") {

    /**
     * Rigourous Tests :-)
     */
    def testOK() {
      val hashA = new TransactionalFixedHashTable[String,String]( 16 )
      val hashB = new TransactionalFixedHashTable[String,String]( 16 )

      hashA.put( "Hi", "Hello" )
      hashB.put( "Bye", "Goodbye" )

      @tailrec
      def swap( keyA: String, keyB: String ): Boolean = {
        atomic {
          hashA.get( keyA ) match {
            case Some( valueA ) => {
              hashB.get( keyB ) match {
                case Some( valueB ) => {
                  hashA.remove( keyA )
                  hashB.remove( keyB )
                  hashA.put( keyB, valueB )
                  hashB.put( keyA, valueA )
                  true
                }
                case None => false // oh well
              }
            }
            case None => false // oh well
          }
        } match {
          case Some( result ) => result
          case None => swap( keyA, keyB ) // transaction failed try again
        }
      }
      assertFalse( swap( "Bye", "Hi" ) )
      assertTrue( swap( "Hi", "Bye" ) )
    }

    def testKO() {
      // todo
      assertTrue(false)
    }
    

}
