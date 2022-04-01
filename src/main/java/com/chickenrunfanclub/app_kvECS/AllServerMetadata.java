package com.chickenrunfanclub.app_kvECS;

import com.chickenrunfanclub.client.KVStore;
import com.chickenrunfanclub.ecs.ECSNode;
import com.chickenrunfanclub.shared.Hasher;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;
import java.util.stream.Collectors;

import static com.chickenrunfanclub.ecs.ECSNode.ECSNodeFlag.STOP;

public class AllServerMetadata {
    private HashMap<String, ECSNode> nodeHashesToServerInfo;
    private static final Logger logger = LogManager.getLogger(AllServerMetadata.class);
    private List<ECSNode> nodesSortedByHash;
    private final static int chainLength = 2;  // number of additional servers -> this is a chain of three


    public AllServerMetadata() {
        nodeHashesToServerInfo = new HashMap<>();
        nodesSortedByHash = new ArrayList<>();
    }

    public AllServerMetadata(HashMap<String, ECSNode> nodeHashesToServerInfo) {
        // this is used for passing metadata around
        this.nodeHashesToServerInfo = nodeHashesToServerInfo;
        nodesSortedByHash = new ArrayList<>(nodeHashesToServerInfo.values());
        nodesSortedByHash.sort(Comparator.comparing(ECSNode::getRangeStart));
    }

    public AllServerMetadata(String pathToConfigFile) {
        nodeHashesToServerInfo = new HashMap<>();
        initServerMetadata(pathToConfigFile);
        // make sure the list is sorted
        nodesSortedByHash = new ArrayList<>(nodeHashesToServerInfo.values());
        nodesSortedByHash.sort(Comparator.comparing(ECSNode::getRangeStart));
    }

    private void initServerMetadata(String file) {
        File f = new File(file);
        String name, host, port;
        try {
            Scanner scanner = new Scanner(f);
            while (scanner.hasNextLine()) {
                String[] line = scanner.nextLine().split(" ");
                name = line[0];
                host = line[1];
                port = line[2];
                String hostPort = host + port;
                String hash = Hasher.hash(hostPort);
                String cacheStrategy = "LRU";
                int cacheSize = 100;

                // note that the server end is set to null. We will fill this in later
                ECSNode node = new ECSNode(host, Integer.parseInt(port), hash, null, true, false, cacheStrategy, cacheSize, name, STOP);
                nodeHashesToServerInfo.put(hash, node);
            }
        } catch (FileNotFoundException e) {
            System.out.println("Configuration file not found");
            e.printStackTrace();
            System.exit(1);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // fill in server hash range ends
        List<String> hashes = new ArrayList<>(nodeHashesToServerInfo.keySet());
        java.util.Collections.sort(hashes);

        for (int i = 0; i < hashes.size(); i++) {
            if (i == hashes.size() - 1) {
                continue;
            }

            ECSNode node = nodeHashesToServerInfo.get(hashes.get(i));
            node.setRangeEnd(hashes.get(i + 1));
        }
        // need to set the last hash to have the first hash as its end
        String firstNodeHash = nodeHashesToServerInfo.get(hashes.get(0)).getRangeStart();
        nodeHashesToServerInfo.get(hashes.get(hashes.size() - 1)).setRangeEnd(hashes.get(0));

//        nodeHashesToServerInfo.forEach((key, node) -> System.out.println(node.getRangeStart() + " " + node.getRangeEnd()));
    }

    public Map<String, ECSNode> getHashToServer() {
        return nodeHashesToServerInfo;
    }

    public ECSNode findServerResponsible(String key, boolean get) {
        // need to add a boolean get param that if true, returns the server at the tail
        if (get) {
            ECSNode headNode = findServerResponsible(key, false);
            return findServerAheadInHashRing(headNode, chainLength);
        }

        return nodeHashesToServerInfo
                .values()
                .stream()
                .filter(node -> node.responsibleFor(key))
                .collect(Collectors.toList())
                .get(0);
    }

    public List<ECSNode> getAllNodesByStatus(ECSNode.ECSNodeFlag status) {
        return nodeHashesToServerInfo
                .values()
                .stream()
                .filter(node -> node.getStatus() == status)
                .collect(Collectors.toList());
    }

    public List<ECSNode> getAllNodes() {
        return new ArrayList<ECSNode>(nodeHashesToServerInfo.values());
    }

    public void updateStatus(ECSNode.ECSNodeFlag originalStatus, ECSNode.ECSNodeFlag targetStatus) {
        List<ECSNode> nodesToUpdate = getAllNodesByStatus(originalStatus);
        List<String> hashes = nodesToUpdate.stream().map(ECSNode::getRangeStart).collect(Collectors.toList());
        hashes.forEach(hash -> {
            ECSNode node = nodeHashesToServerInfo.get(hash);
            node.setStatus(targetStatus);
        });
    }

    public ECSNode getFirstByStatus(ECSNode.ECSNodeFlag status) {
//        nodeHashesToServerInfo.forEach((key, node) -> System.out.println(node.getName() + " " + node.getStatus()));
        return nodeHashesToServerInfo
                .values()
                .stream()
                .filter(node -> node.getStatus() == status)
                .findFirst().get();
    }

    public ECSNode getFirstByEndRange(String rangeEnd) {
        return nodeHashesToServerInfo
                .values()
                .stream()
                .filter(node -> Objects.equals(node.getRangeEnd(), rangeEnd))
                .findFirst().get();
    }

    public void updateNodeStatus(ECSNode node, ECSNode.ECSNodeFlag status) {
        nodeHashesToServerInfo.get(node.getRangeStart()).setStatus(status);
    }

    public void reset() {
        nodeHashesToServerInfo = new HashMap<>();
    }

    private void addNodesSortedByHash(ECSNode node) {
        nodesSortedByHash.add(node);
        nodesSortedByHash.sort(Comparator.comparing(ECSNode::getRangeStart));
    }

    private void removeNodesSortedByHash(ECSNode node) {
        nodesSortedByHash = nodesSortedByHash
                .stream()
                .filter(x -> x != node)
                .collect(Collectors.toList());

        nodesSortedByHash.sort(Comparator.comparing(ECSNode::getRangeStart));
    }


    public void addNode(ECSNode node) {
        String hash = Hasher.hash(node.getHost() + node.getPort());

        if (nodeHashesToServerInfo.size() == 0) {
            node.setRangeEnd(hash);
            node.setRangeStart(hash);

            // need to update the map as well as the sorted list
            nodeHashesToServerInfo.put(hash, node);
            addNodesSortedByHash(node);
            return;
        }

        ECSNode previous = findServerResponsible(node.getHost() + node.getPort(), false);

        // insert new node into the hash ring
        node.setRangeStart(hash);
        node.setRangeEnd(previous.getRangeEnd());
        nodeHashesToServerInfo.put(hash, node);

        // update sorted hashes
        addNodesSortedByHash(node);
        removeNodesSortedByHash(previous);

        // update previous nodes range to end at the new node
        nodeHashesToServerInfo.get(previous.getRangeStart()).setRangeEnd(hash);
        addNodesSortedByHash(nodeHashesToServerInfo.get(previous.getRangeStart()));
    }

    public void removeNode(ECSNode node) {
        if (nodeHashesToServerInfo.size() == 1) {
            // don't allow an empty ring
            return;
        }

        ECSNode previous = getFirstByEndRange(node.getRangeStart());

        // update sorted hashes
        removeNodesSortedByHash(node);
        removeNodesSortedByHash(nodeHashesToServerInfo.get(previous.getRangeStart()));

        nodeHashesToServerInfo.get(previous.getRangeStart()).setRangeEnd(node.getRangeEnd());
        nodeHashesToServerInfo.remove(node.getRangeStart());

        addNodesSortedByHash(nodeHashesToServerInfo.get(previous.getRangeStart()));
    }

    public void print() {
        nodeHashesToServerInfo.forEach((key, value) -> logger.info(value.getPort() + ": [" + value.getRangeStart() + ", " + value.getRangeEnd() + "]"));
    }

    public void broadcastMetadata() {
        // only broadcast to servers that are running
        List<ECSNode> runningServers = getAllNodesByStatus(ECSNode.ECSNodeFlag.START);

        runningServers.forEach(node -> {
            KVStore kvClient = new KVStore(node.getHost(), node.getPort());
            try {
                kvClient.updateAllMetadata(this);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    public ECSNode findServerAheadInHashRing(ECSNode node, int ahead) {
        int index = nodesSortedByHash.indexOf(node);
        index = (index + ahead) % nodesSortedByHash.size();
        return nodesSortedByHash.get(index);
    }

    public List<ECSNode> findGetServersResponsible(String key) {
        /*
         * This is used for get since clients are allowed to query at any point along the chain
         * */
        ECSNode chainHead = findServerResponsible(key, false);
        List<ECSNode> responsibleServers = new ArrayList<>();
        for(int i = 1; i < chainLength + 1; i++) {
            responsibleServers.add(findServerAheadInHashRing(chainHead, i));
        }
        responsibleServers.add(chainHead);
        return responsibleServers;

    }
}
