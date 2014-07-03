package com.facebook.LinkBench;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import java.net.UnknownHostException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author davide
 */
public class MongoDbTestConfig {

    static String host = "localhost";
    static int port = 27017;
    static String linkCollection = "test_linkCollection";
    static String countCollection = "test_countCollection";
    static String nodeCollection = "test_nodeCollection";

    public static void fillMongoDbTestServerProps(Properties props) {
        props.setProperty(Config.LINKSTORE_CLASS,
            LinkStoreMongoDb.class.getName());
        props.setProperty(Config.NODESTORE_CLASS,
            LinkStoreMongoDb.class.getName());
        props.setProperty(LinkStoreMongoDb.CONFIG_HOST, host);
        props.setProperty(LinkStoreMongoDb.CONFIG_PORT, Integer.toString(port));
        props.setProperty(Config.LINK_TABLE, linkCollection);
        props.setProperty(Config.COUNT_TABLE, countCollection);
        props.setProperty(Config.NODE_TABLE, nodeCollection);
    }

    static MongoClient createConnection(String testDB)
            throws UnknownHostException {
            return new MongoClient(host, port);
    }

    static void createTestTables(MongoClient conn, String testDB)
            throws Exception{
        DB db = conn.getDB(testDB);

        DBCollection link_coll = db.createCollection(
                MongoDbTestConfig.linkCollection, null);
        DBCollection count_coll = db.createCollection(
                MongoDbTestConfig.countCollection, null);
        DBCollection node_coll = db.createCollection(
                MongoDbTestConfig.nodeCollection, null);
    }

    static void dropTestTables(MongoClient conn, String testDB)
            throws Exception {
        DB db = conn.getDB(testDB);

        DBCollection link_coll = db.getCollection(linkCollection);
        link_coll.dropIndexes();
        link_coll.drop();

        DBCollection count_coll = db.getCollection(countCollection);
        count_coll.dropIndexes();
        count_coll.drop();

        DBCollection node_coll = db.getCollection(nodeCollection);
        node_coll.dropIndexes();
        node_coll.drop();
    }
}
