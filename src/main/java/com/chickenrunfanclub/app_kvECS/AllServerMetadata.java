package com.chickenrunfanclub.app_kvECS;

import com.chickenrunfanclub.ecs.ECSNode;
import com.chickenrunfanclub.shared.Hasher;

import javax.print.DocFlavor;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;
import java.util.stream.Collectors;

import static com.chickenrunfanclub.ecs.ECSNode.ECSNodeFlag.IDLE;
import static com.chickenrunfanclub.ecs.ECSNode.ECSNodeFlag.STOP;

public class AllServerMetadata {
    private final HashMap<String, ECSNode> nodeHashesToServerInfo;

    public AllServerMetadata(HashMap<String, ECSNode> nodeHashesToServerInfo) {
        // this is used for passing metadata around
        this.nodeHashesToServerInfo = nodeHashesToServerInfo;
    }

    public AllServerMetadata(String pathToConfigFile) {
        nodeHashesToServerInfo = new HashMap<>();
        initServerMetadata(pathToConfigFile);
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

    public ECSNode findServerResponsible(String key) {
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

    public void updateNodeStatus(ECSNode node, ECSNode.ECSNodeFlag status) {
        nodeHashesToServerInfo.get(node.getRangeStart()).setStatus(status);
    }
}
