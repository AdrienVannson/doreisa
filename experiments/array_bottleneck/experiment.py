"""
Run an experiment to dertermine the maximum number of references that a single node can process.

This script must be executed from the root of the repository.
"""

import execo
import execo_g5k
import execo_engine
import sys
import time


def run_experiment(nb_reserved_nodes: int, nb_workers: int) -> None:
    print("Starting experiment with", nb_reserved_nodes, nb_workers)

    # Reserve the resources
    jobs = execo_g5k.oarsub([(execo_g5k.OarSubmission(f"nodes={nb_reserved_nodes}", walltime=30*60), "luxembourg")])
    job_id, site = jobs[0]

    # Get useful stats
    nodes = execo_g5k.get_oar_job_nodes(job_id, site)
    head_node, nodes = nodes[0], nodes[1:]

    cores_per_node = execo_g5k.get_host_attributes(head_node)['architecture']['nb_cores']
    total_simulation_cores = cores_per_node * len(nodes)

    print("\tSimulation cores available: ", total_simulation_cores)

    # Start the head node
    head_node_cmd = execo.SshProcess(f'singularity exec doreisa/docker/images/doreisa-analytics.sif bash -c "cd doreisa; ray start --head --port=4242; python3 experiments/array_bottleneck/head.py {len(nodes)}"', head_node)
    head_node_cmd.start()

    time.sleep(5)

    print("\tHead node started")

    # Start the simulation nodes
    for node in nodes:
        node_cmd = execo.SshProcess(f"""singularity exec doreisa/docker/images/doreisa-simulation.sif bash -c "ray start --address='{head_node.address}:4242'"; sleep infinity """, node)
        node_cmd.start()

    # Write the hostfile
    with open("hostfile", "w") as hostfile:
        for node in nodes:
            for _ in range(cores_per_node):
                hostfile.write(f"{node.address}\n")

    # Wait for everything to start
    time.sleep(10)

    # Run the simulation
    cmd_simulation = execo.SshProcess(f"""bash -c "cd doreisa && singularity exec docker/images/doreisa-simulation.sif mpirun -n {nb_workers} --oversubscribe --hostfile hostfile singularity exec docker/images/doreisa-simulation.sif python3 experiments/array_bottleneck/worker.py" """, head_node)
    cmd_simulation.start()
    cmd_simulation.wait()

    # Release the ressources
    execo_g5k.oardel(jobs)

# First, test if the bottleneck can come from the workers
for n in [1, 2, 4, 8, 16, 32, 64, 128, 256, 512]:
    run_experiment(2, n)

# Then, measure the performance
for n in [1, 2, 4, 8, 16, 32, 64, 128, 256, 512]:
    run_experiment(6, n)
