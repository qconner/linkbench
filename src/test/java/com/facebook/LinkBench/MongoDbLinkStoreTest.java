package com.facebook.LinkBench;


import com.facebook.LinkBench.testtypes.MongoDbTest;
import com.mongodb.MongoClient;
import java.io.IOException;
import java.util.Properties;
import org.junit.experimental.categories.Category;

/**
 * Test the MongoDB LinkStore implementation.
 */
@Category(MongoDbTest.class)
public class MongoDbLinkStoreTest extends LinkStoreTestBase {
    
    private MongoClient conn;
    
    /* Properties for last initStore call */
    private Properties currProps;
    
    @Override
    protected long getIDCount() {
        /* Make this test smaller so that it doesn't take too long */
        return 5000;
    }
    
    @Override
    protected int getRequestCount() {
        /* Fewer requests to keep the test quick */
        return 10000;
    }
    
    protected Properties basicProps() {
        Properties props = super.basicProps();
        MongoDbTestConfig.fillMongoDbTestServerProps(props);
        return props;
    }
    
    @Override
    protected void initStore(Properties props) throws IOException, Exception {
        this.currProps = (Properties)props.clone();
        
        if (conn != null) {
            conn.close();
        }
        
        conn = MongoDbTestConfig.createConnection(testDB);
        MongoDbTestConfig.dropTestTables(conn, testDB);
        MongoDbTestConfig.createTestTables(conn, testDB);
    }
    
    @Override
    public DummyLinkStore getStoreHandle(boolean initialize)
        throws IOException, Exception {
        DummyLinkStore result = new DummyLinkStore(new LinkStoreMongoDb());
        if (initialize) {
            result.initialize(currProps, Phase.REQUEST, 0);
        }
        return result;
    }
    
    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        MongoDbTestConfig.dropTestTables(conn, testDB);
        conn.close();
    }
}
