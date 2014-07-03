package com.facebook.LinkBench;

import com.facebook.LinkBench.testtypes.MongoDbTest;
import com.mongodb.MongoClient;
import java.io.IOException;
import java.util.Properties;
import org.junit.experimental.categories.Category;

/**
 *
 * @author davide
 */
@Category(MongoDbTest.class)
public class MongoDbNodeStoreTest extends NodeStoreTestBase {

    MongoClient conn;
    Properties currProps;
    
    @Override
    protected Properties basicProps() {
        Properties props = super.basicProps();
        MongoDbTestConfig.fillMongoDbTestServerProps(props);
        return props;
    }
    
    @Override
    protected void initNodeStore (Properties props)
            throws Exception, IOException {
        currProps = props;
        conn = MongoDbTestConfig.createConnection(testDB);
        MongoDbTestConfig.dropTestTables(conn, testDB);
        MongoDbTestConfig.createTestTables(conn, testDB);
    }
    
    @Override
    protected NodeStore getNodeStoreHandle(boolean initialize) 
        throws Exception, IOException {
        DummyLinkStore result = new DummyLinkStore(new LinkStoreMongoDb());
        if (initialize) {
            result.initialize(currProps, Phase.REQUEST, 0);
        }
        return result;
    }
}
