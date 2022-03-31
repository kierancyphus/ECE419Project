import subprocess
import os
import csv

num_servers = [3, 5, 10, 20, 50]
num_clients = [1, 5, 20, 50, 100]
put_ratios = [0.2, 0.5, 0.8]

for server in num_servers:
    for client in num_clients:
        for ratio in put_ratios:
            print(server, client, ratio)