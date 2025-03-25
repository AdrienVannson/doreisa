import execo
import execo_g5k
import execo_engine
import sys
import time

# Reserve the resources
jobs = execo_g5k.oarsub([(execo_g5k.OarSubmission("nodes=3", walltime=3600), "luxembourg")])
job_id, site = jobs[0]

# Get useful stats
nodes = execo_g5k.get_oar_job_nodes(job_id, site)
head_node, nodes = nodes[0], nodes[1:]

cores_per_node = execo_g5k.get_host_attributes(head_node)['architecture']['nb_cores']
total_simulation_cores = cores_per_node * len(nodes)

print("Simulation cores available: ", total_simulation_cores)

# Start the head node
head_node_cmd = execo.SshProcess('singularity exec doreisa/docker/images/doreisa-analytics.sif bash -c "ray start --head --port=4242; python3 doreisa/head.py"', head_node)
head_node_cmd.start()

time.sleep(10)

print("Head node started")

# Start the simulation nodes
starting_nodes = []
for node in nodes:
    node_cmd = execo.SshProcess(f"""singularity exec doreisa/docker/images/doreisa-simulation.sif bash -c "ray start --address='{head_node.address}:4242'"; sleep infinity """, node)
    node_cmd.start()
    starting_nodes.append(node_cmd)

# Write the hostfile
with open("hostfile", "w") as hostfile:
    for node in nodes:
        for _ in range(cores_per_node):
            hostfile.write(f"{node.address}\n")

# Wait for everything to start
time.sleep(10)

# Run the simulation
cmd_simulation = execo.SshProcess(f"""bash -c "cd doreisa && singularity exec docker/images/doreisa-simulation.sif mpirun -n 4 --hostfile hostfile singularity exec docker/images/doreisa-simulation.sif ./simulation" """, head_node)
cmd_simulation.start()

# Do whatever we want
breakpoint()

# Release the ressources
execo_g5k.oardel(jobs)
