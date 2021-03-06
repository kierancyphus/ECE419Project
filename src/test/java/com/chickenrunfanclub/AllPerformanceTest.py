import subprocess
import csv

num_servers = [3, 5, 10, 20, 50]
num_clients = [1, 5, 20, 50, 100]
put_ratios = [0.2, 0.5, 0.8]

results = []
for server in num_servers:
    for client in num_clients:
        for ratio in put_ratios:
            proc = subprocess.run(['./gradlew', 'perftest',
                                   "./src/test/resources/servers_perftest.cfg",
                                   str(100), str(server), str(client), str(ratio)],
                                  capture_output=True)
            output = proc.stdout.decode()
            results.append(output.split("\n")[:-1])

with open("perfTest.txt", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerows(results)
