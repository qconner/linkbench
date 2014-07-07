/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.facebook.LinkBench;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.concurrent.AtomicInitializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 *
 * @author davide
 */
public class LinkStoreMongoDb extends GraphStore {

    /* MongoDB database server configuration keys */
    public static final String CONFIG_HOST = "host";
    public static final String CONFIG_PORT = "port";
    public static final String CONFIG_CONN_PER_HOST = "conn_per_host";
    public static final String CONFIG_SOCKTIMEOUT = "socktimeout";
    public static final String CONFIG_WRITECONCERN = "writeconcern";
    public static final String CONFIG_DB = "test";

    public static final int DEFAULT_BULKINSERT_SIZE = 1024;

    String host;
    String port;
    String connPerHost;
    String sockTimeout;
    String wc;
    String defaultDB;
    String countCollection;
    String linkCollection;
    String nodeCollection;

    Level debuglevel;

    MongoClient conn;

    private Phase phase;

    int bulkInsertSize = DEFAULT_BULKINSERT_SIZE;

    private final Logger logger = Logger.getLogger(ConfigUtil.LINKBENCH_LOGGER);

    /* Generator for _id */
    AtomicInteger id_gen;

    public LinkStoreMongoDb() {
        super();
        id_gen = new AtomicInteger();
    }

    public LinkStoreMongoDb(Properties props) throws IOException, Exception {
        super();
        initialize(props, Phase.LOAD, rangeLimit);
    }

    @Override
    public void initialize(Properties p, Phase currentPhase, int threadId)
      throws IOException, Exception {

        countCollection = ConfigUtil.getPropertyRequired(p,
            Config.COUNT_TABLE);
        if (countCollection.equals("")) {
            String msg = "Error! " + Config.COUNT_TABLE + " is empty!"
                    + "Please check configuration file.";
            logger.error(msg);
            throw new RuntimeException(msg);
        }

        nodeCollection = p.getProperty(Config.NODE_TABLE);
        if (nodeCollection.equals("")) {
            String msg = "Error! " + Config.NODE_TABLE + " is empty!"
                    + "Please check configuration file.";
            logger.error(msg);
            throw new RuntimeException(msg);
        }


        host = ConfigUtil.getPropertyRequired(p, CONFIG_HOST);
        port = ConfigUtil.getPropertyRequired(p, CONFIG_PORT);
        connPerHost = ConfigUtil.getPropertyRequired(p, CONFIG_CONN_PER_HOST);
        sockTimeout = ConfigUtil.getPropertyRequired(p, CONFIG_SOCKTIMEOUT);
        wc = ConfigUtil.getPropertyRequired(p, CONFIG_WRITECONCERN);
        defaultDB = ConfigUtil.getPropertyRequired(p, Config.DBID);

        debuglevel = ConfigUtil.getDebugLevel(p);
        phase = currentPhase;

        try {
            openConnection();
        } catch (Exception e) {
            logger.error("error connecting to database:", e);
            throw e;
        }

        linkCollection = ConfigUtil.getPropertyRequired(p, Config.LINK_TABLE);
    }

    private void openConnection() throws Exception {
        MongoClientOptions opts = new MongoClientOptions.Builder().
                connectionsPerHost(Integer.parseInt(connPerHost)).
                socketTimeout(Integer.parseInt(sockTimeout)).
                writeConcern(toWriteConcern(wc)).build();
        ServerAddress s_addr = new ServerAddress(host, Integer.parseInt(port));

        conn = new MongoClient(s_addr, opts);
        assert(conn != null);
    }

    private WriteConcern toWriteConcern(String wc_type) {
        WriteConcern wc;

        switch (wc_type) {
                case "SAFE":
                    wc = WriteConcern.SAFE;
                    break;
                case "ACKNOWLEDGED":
                    wc = WriteConcern.ACKNOWLEDGED;
                    break;
                case "NORMAL":
                    wc = WriteConcern.NORMAL;
                    break;
                default:
                    wc = WriteConcern.NONE;
                    break;
        }

        return wc;
    }

    @Override
    public void close() {
        try {
            if (conn != null) {
                conn.close();
            }
        } catch (Throwable e) {
            logger.error("Error while closing MongoDB connection: ", e);
        }
    }

    @Override
    public void clearErrors(int threadID) {
        logger.info(("Reopening MongoDB connection in threadID" + threadID));

        try {
            if (conn != null) {
                conn.close();
            }

            openConnection();
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean addLink(String dbid, Link l, boolean noinverse)
            throws Exception {
        while (true) {
            try {
                return addLinkImpl(dbid, l, noinverse);
            } catch (Exception e) {
                logger.error("Error in addLink: ", e);
                e.printStackTrace();
                return false;
            }
        }
    }

    private boolean addLinkImpl(String dbid, Link l, boolean noinverse)
            throws Exception {

        if (Level.DEBUG.isGreaterOrEqual(debuglevel)) {
            logger.debug("addLink " + l.id1 +
                         "." + l.id2 +
                         "." + l.link_type);
        }

        int ndocs = addLinksNoCount(dbid, Collections.singletonList(l));

        if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
            logger.trace("ndocs = " + ndocs);
        }

        /* XXX: fixme, Insert or update */
        return true;
    }

    private int addLinksNoCount(String dbid, List<Link> links)
            throws Exception {

        if (links.size() == 0)
            return 0;

        /* XXX: ensureIndex() ? */
        DB db = conn.getDB(defaultDB);
        DBCollection coll = db.getCollection(linkCollection);

        BasicDBObject[] docs = new BasicDBObject[links.size()];
        int i = 0;

        List<DBObject> documents = new ArrayList<>();

        for (Link l : links) {
            BasicDBObject doc = new BasicDBObject("id1", l.id1)
                    .append("id2", l.id2)
                    .append("link_type", l.link_type)
                    .append("visibility", l.visibility)
                    .append("data", l.data.toString())
                    .append("version", l.version);
            documents.add(doc);

            if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
                logger.trace(doc.toString());
            }
        }

        /* XXX: check the write actually succedes */
        WriteResult insert = coll.insert(documents);

        return links.size();
    }

    @Override
    public boolean deleteLink(String dbid, long id1, long link_type, long id2,
        boolean noinverse, boolean expunge) throws Exception {
        while (true) {
            try {
                return deleteLinkImpl(dbid, id1, link_type, id2, noinverse,
                    expunge);
            } catch (Exception e) {
                throw e;
            }
        }
    }

    private boolean deleteLinkImpl(String dbid, long id1, long link_type,
            long id2, boolean noinverse, boolean expunge) {

        if (Level.DEBUG.isGreaterOrEqual(debuglevel)) {
            logger.debug("deleteLink " + id1 +
                    "." + id2 +
                    "." + link_type);
        }

        /*
         * First do a select to check if the link is not there, is there and
         * hidden, or is there and visible;
         * Result could be either NULL, VISIBILITY_HIDDEN or VISIBILITY_DEFAULT.
         * In case of VISIBILITY_DEFAULT, later we need to mark the link as
         * hidden, and update counttable.
         * XXX: atomicity?
         */
        DB db = conn.getDB(dbid);
        DBCollection coll = db.getCollection(linkCollection);

        BasicDBObject clause_id1 = new BasicDBObject("id1", id1);
        BasicDBObject clause_id2 = new BasicDBObject("id2", id2);
        BasicDBObject clause_link = new BasicDBObject("link_type", link_type);
        BasicDBList and = new BasicDBList();
        and.add(clause_id1);
        and.add(clause_id2);
        and.add(clause_link);
        BasicDBObject query = new BasicDBObject("$and", and);

        if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
            logger.trace(query.toString());
        }

        DBCursor cur = coll.find(query);

        int visibility = -1;
        boolean found = false;
        while (cur.hasNext()) {
            visibility =  (int) cur.next().get("visibility");
            found = true;
        }

        if (!found) {
            /* do nothing */
        }
        else if (visibility == VISIBILITY_HIDDEN && !expunge) {
            /* do nothing */
        }
        else {
            /* Only updateCount if the link is present and visible */
            boolean updateCount = (visibility != VISIBILITY_HIDDEN);

            if (!expunge) {
                /* XXX: CHECK return value? */
                BasicDBObject set = new BasicDBObject("$set",
                    new BasicDBObject("visibility", VISIBILITY_HIDDEN));
                coll.update(query, set);
                if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
                    logger.trace("query: " + query.toString() +
                            " update: " + set.toString());
                }
            }
            else {
                /* XXX: CHECK return value? */
                coll.remove(query);
                if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
                    logger.trace("delete: " + query.toString());
                }
            }
            long currentTime = (new Date()).getTime();
            DBCollection count_coll = db.getCollection(countCollection);

            /* XXX: Update count table */
        }
        return false;
    }

    @Override
    public boolean updateLink(String dbid, Link a, boolean noinverse)
            throws Exception {
        /* Retry logic is in addLink */
        boolean added = addLink(dbid, a, noinverse);
        return !added; /* return true if updated instead of added */
    }

    @Override
    public Link getLink(String dbid, long id1, long link_type, long id2)
            throws Exception {
        while (true) {
            try {
                return getLinkImpl(dbid, id1, link_type, id2);
            }
            catch (Exception e) {
                throw e;
            }
        }
    }

    private Link getLinkImpl(String dbid, long id1, long link_type, long id2)
            throws Exception {
        Link res[] = multigetLinks(dbid, id1, link_type, new long[] {id2});
        if (res == null) return null;
        assert(res.length <= 1);
        return res.length == 0 ? null : res[0];
    }

    @Override
    public Link[] multigetLinks(String dbid, long id1, long link_type,
        long[] id2s) throws Exception {
        while (true) {
            try {
                return multigetLinksImpl(dbid, id1, link_type, id2s);
            } catch (Exception e) {
                throw e;
            }
        }
    }

    private Link[] multigetLinksImpl(String dbid, long id1, long link_type,
            long[] id2s) throws Exception {

        DB db = conn.getDB(dbid);
        DBCollection coll = db.getCollection(linkCollection);

        BasicDBObject clause_id1 = new BasicDBObject("id1", id1);
        BasicDBObject clause_link = new BasicDBObject("link_type", link_type);

        ArrayList<String> in_array = new ArrayList<>();
        for (long id2: id2s) {
            in_array.add(Long.toString(id2));
        }

        BasicDBObject clause_in = new BasicDBObject("id2",
            new BasicDBObject("$in", in_array));

        BasicDBList and = new BasicDBList();
        and.add(clause_id1);
        and.add(clause_link);
        and.add(clause_in);

        BasicDBObject query = new BasicDBObject("$and", and);

        DBCursor cur = coll.find(query);

        if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
            logger.trace("query: " + query.toString());
        }

        ArrayList<Link> links = new ArrayList<>();

        while (cur.hasNext()) {
            Link l = createLinkFromDoc((BasicDBObject) cur.next());
            links.add(l);
        }
        return (Link[]) links.toArray();
    }

    private Link createLinkFromDoc(BasicDBObject doc)
            throws Exception {
        Link l = new Link();
        l.id1 = doc.getLong("id1");
        l.id2 = doc.getLong("id2");
        l.link_type = doc.getLong("link_type");
        l.visibility = (byte) doc.getInt("visibility");
        /* XXX: BSON doesn't support array of bytes as primitive type */
        l.data = doc.getString("data").getBytes();
        l.version = doc.getInt("version");
        return l;
    }

    @Override
    public Link[] getLinkList(String dbid, long id1, long link_type)
            throws Exception {
        return getLinkList(dbid, id1, link_type, 0, Long.MAX_VALUE, 0,
            rangeLimit);
    }

    @Override
    public Link[] getLinkList(String dbid, long id1, long link_type,
            long minTimestamp, long maxTimestamp, int offset, int limit)
            throws Exception {
        while (true) {
            try {
                return getLinkListImpl(dbid, id1, link_type, minTimestamp,
                    maxTimestamp, offset, limit);
            } catch (Exception e) {
                throw e;
            }
        }
    }

    private Link[] getLinkListImpl(String dbid, long id1, long link_type,
        long minTimestamp, long maxTimestamp,
        int offset, int limit)
            throws Exception {

        DB db = conn.getDB(dbid);
        DBCollection coll = db.getCollection(linkCollection);

        /* XXX: order by, limit */
        /* XXX: implement */
        BasicDBObject id_clause = new BasicDBObject();
        BasicDBObject link_clause = new BasicDBObject();
        BasicDBObject time_clause = new BasicDBObject();
        BasicDBObject visibility = new BasicDBObject();
        BasicDBObject query = new BasicDBObject();

        if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
            logger.trace("query: " + query.toString());
        }

        DBCursor cur = coll.find(query);
        ArrayList<Link> links = new ArrayList<>();
        int ndocs = 0;

        while (cur.hasNext()) {
            Link l = createLinkFromDoc((BasicDBObject) cur.next());
            links.add(l);
            ndocs++;
        }

        if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
            logger.trace("Range lookup result: " + id1 + "," + link_type +
                    " is " + ndocs);
        }

        if (ndocs == 0) {
            return null;
        }
        return links.toArray(new Link[ndocs]);
    }

    @Override
    public long countLinks(String dbid, long id1, long link_type)
            throws Exception {
        while (true) {
            try {
                return countLinksImpl(dbid, id1, link_type);
            } catch (Exception e) {
                throw e;
            }
        }
    }

    private long countLinksImpl(String dbid, long id1, long link_type)
        throws Exception {
        long count = 0;

        DB db = conn.getDB(dbid);
        DBCollection coll = db.getCollection(countCollection);
        boolean found = false;

        BasicDBObject id_clause = new BasicDBObject("_id", id1);
        BasicDBObject link_clause = new BasicDBObject("link_type", link_type);
        BasicDBList and = new BasicDBList();
        and.add(id_clause);
        and.add(link_clause);
        BasicDBObject query = new BasicDBObject("$and", and);

        DBCursor cur = coll.find(query);

        count = 0;
        while (cur.hasNext()) {
            cur.next();
            count++;
            if (count > 1) {
                logger.trace("Count query 2nd doc!: " + id1 + "," + link_type);
            }
        }

        if (Level.TRACE.isGreaterOrEqual((debuglevel))) {
            logger.trace("Count result: " + id1 + "," + link_type +
                    " is " + found + " and " + count);
        }

        return count;
    }

    @Override
    public int bulkLoadBatchSize() {
        return bulkInsertSize;
    }

    @Override
    public void addBulkLinks(String dbid, List<Link> links, boolean noinverse)
            throws Exception {
        while (true) {
            try {
                addBulkLinksImpl(dbid, links, noinverse);
                return;
            } catch (Exception e) {
                throw e;

            }
        }
    }

    private void addBulkLinksImpl(String dbid, List<Link> links,
        boolean noinverse) throws Exception {
        if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
            logger.trace("addBulkLinks: " + links.size() + " links");
        }

        addLinksNoCount(dbid, links);
    }

    private void checkNodeTableConfigured() throws Exception {
        if (nodeCollection == null) {
            throw new Exception("Nodetable not specified: cannot perform node" +
                " operation");
        }
    }

    @Override
    public void addBulkCounts(String dbid, List<LinkCount> counts)
        throws Exception {
        while (true) {
            try {
                addBulkCountsImpl(dbid, counts);
            } catch (Exception e) {
                throw e;
            }
        }
    }

    private void addBulkCountsImpl(String dbid, List<LinkCount> counts)
            throws Exception {

        if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
            logger.trace(("addBulkCounts: " + counts.size() + " link counts"));
        }

        if (counts.isEmpty())
            return;

        DB db = conn.getDB(dbid);
        DBCollection coll = db.getCollection(countCollection);

        /* XXX: Mimic MySQL REPLACE behaviour with update(upsert == True) */
        for (LinkCount count: counts) {
            BasicDBObject q_obj = new BasicDBObject("id1", count.id1)
                    .append("link_type", count.link_type)
                    .append("count", count.count)
                    .append("time", count.time)
                    .append("version", count.version);

            if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
                logger.trace(q_obj.toString());
            }

            coll.update(q_obj, q_obj, true /* upsert */, false /* multi */);

        }
    }

    @Override
    public void resetNodeStore(String dbid, long startID) throws Exception {
        checkNodeTableConfigured();
        DB db = conn.getDB(dbid);
        DBCollection coll = db.getCollection(nodeCollection);

        coll.dropIndexes();
        coll.drop();
    }

    @Override
    public long addNode(String dbid, Node node) throws Exception {
        while (true) {
            try {
                return addNodeImpl(dbid, node);
            } catch (Exception e) {
                throw e;
            }
        }
    }

    private long addNodeImpl(String dbid, Node node) throws Exception {
        long ids[] = bulkAddNodes(dbid, Collections.singletonList(node));
        assert(ids.length == 1);
        return ids[0];
    }

    public long[] bulkAddNodes(String dbid, List<Node> nodes) throws Exception {
        while (true) {
            try {
                return bulkAddNodesImpl(dbid, nodes);
            } catch (Exception e) {
                throw e;
            }
        }
    }

    private long[] bulkAddNodesImpl(String dbid, List<Node> nodes)
            throws Exception {
        checkNodeTableConfigured();

        DB db = conn.getDB(dbid);
        DBCollection coll = db.getCollection(nodeCollection);

        List<DBObject> documents = new ArrayList<>();

        for (Node node: nodes) {
            BasicDBObject doc = new BasicDBObject("_id", id_gen.addAndGet(1))
                    .append("type", node.type)
                    .append("version", node.version)
                    .append("time", node.time)
                    .append("data", node.data.toString());
            documents.add(doc);

            if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
                logger.trace(doc.toString());
            }
        }

        /* XXX: check the write actually succedes */
        WriteResult insert = coll.insert(documents);

        long[] id_arr = new long[documents.size()];
        int i = 0;

        for (DBObject obj: documents) {
            BasicDBObject doc = (BasicDBObject)obj;
            id_arr[i] = doc.getLong("_id");
            i++;
        }

        return id_arr;
    }

    @Override
    public Node getNode(String dbid, int type, long id) throws Exception {
        while (true) {
            try {
                return getNodeImpl(dbid, type, id);
            } catch (Exception e) {
                throw e;
            }
        }
    }

    private Node getNodeImpl(String dbid, int type, long id)
            throws Exception {
        checkNodeTableConfigured();

        DB db = conn.getDB(dbid);
        DBCollection coll = db.getCollection(nodeCollection);
        BasicDBObject query = new BasicDBObject("_id", id);

        DBCursor cur = coll.find(query);
        Node res = null;
        boolean found = false;
        while (cur.hasNext()) {
            /* XXX: Project only relevant fields */
            if (found == false) {
                found = true;
                BasicDBObject res_obj = (BasicDBObject) cur.next();
                res = new Node(res_obj.getLong("_id"), res_obj.getInt("type"),
                        res_obj.getLong("version"), res_obj.getInt("time"),
                        res_obj.getString("data").getBytes());
            }
            else {
                /* Check that multiple docs weren't returned */
                assert(cur.next() == null);
                /* XXX: NEVER REACHED */
                return null;
            }
        }

        return res;
    }

    @Override
    public boolean updateNode(String dbid, Node node) throws Exception {
        while (true) {
            try {
                return updateNodeImpl(dbid, node);
            } catch (Exception e) {
                throw e;
            }
        }
    }

    private boolean updateNodeImpl(String dbid, Node node) throws Exception {
        checkNodeTableConfigured();

        DB db = conn.getDB(dbid);
        DBCollection coll = db.getCollection(nodeCollection);

        BasicDBObject id_clause = new BasicDBObject("_id", node.id);
        BasicDBObject type_clause = new BasicDBObject("type", node.type);
        BasicDBList and = new BasicDBList();
        and.add(id_clause);
        and.add(type_clause);
        BasicDBObject query = new BasicDBObject("$and", and);

        BasicDBObject set = new BasicDBObject("$set",
                    new BasicDBObject("version", node.version)
                        .append("time", node.time)
                        .append("data", node.data.toString()));

        if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
            logger.trace(set.toString());
        }

        WriteResult w_res = coll.update(set, set, false, true,
                toWriteConcern(CONFIG_WRITECONCERN));

        /* XXX: are we interested in the number of documents modified or
         * the number of documents matched?
         */
        int ndocs = w_res.getN();
        if (ndocs == 1) {
            return true;
        }
        else if (ndocs == 0) {
            return false;
        }
        else {
            throw new Exception("Did not expect " + ndocs +  "affected " +
            "documents: only expected update to affect at most one document");
        }
    }

    @Override
    public boolean deleteNode(String dbid, int type, long id) throws Exception {
        while (true) {
            try {
                return deleteNodeImpl(dbid, type, id);
            } catch (Exception e) {
                throw e;
            }
        }
    }

    private boolean deleteNodeImpl(String dbid, int type, long id)
            throws Exception {
        DB db = conn.getDB(dbid);
        DBCollection coll = db.getCollection(nodeCollection);

        if (Level.DEBUG.isGreaterOrEqual(debuglevel)) {
            logger.debug("deleteNode " + id);
        }

        BasicDBObject id_clause = new BasicDBObject("_id", id);
        BasicDBObject type_clause = new BasicDBObject("type", type);
        BasicDBList and = new BasicDBList();
        and.add(id_clause);
        and.add(type_clause);
        BasicDBObject query = new BasicDBObject("$and", and);

        WriteResult w_res = coll.remove(query);
        int ndocs = w_res.getN();

        if (ndocs == 0) {
            return false;
        }
        else if (ndocs == 1) {
            return true;
        }
        else {
            throw new Exception(ndocs + " documents modified on delete: " +
                "should delete at most one");
        }
    }

}

