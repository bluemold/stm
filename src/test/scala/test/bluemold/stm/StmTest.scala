package test.bluemold.stm

import junit.framework._;
import Assert._;

object StmTest {
    def suite: Test = {
        val suite = new TestSuite(classOf[StmTest]);
        suite
    }

    def main(args : Array[String]) {
        junit.textui.TestRunner.run(suite);
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
      // todo
      assertTrue(true)
    }
    def testKO() {
      // todo
      assertTrue(false)
    }
    

}
