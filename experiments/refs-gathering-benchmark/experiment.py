"""
Run an experiment to dertermine the maximum number of references that a single node can process.

This script must be executed from the root of the repository.
"""

import execo
import execo_g5k
import execo_engine
import time


def run_experiment(nb_reserved_nodes: int, nb_workers: int, nb_chunks_sent: int) -> None:
    """
    Params:
        nb_reserved_nodes: The number of nodes to reserve on the Grid'5000 platform
        nb_workers: The number of MPI processes to use
        nb_chunks_sent: The number of chunks to send to the head node by each worker at each iteration
    """

    with open("experiments/refs-gathering-benchmark/measurements.txt", "a") as f:
        f.write(f"{nb_reserved_nodes} ")

    print("Starting experiment with", nb_reserved_nodes, nb_workers, nb_chunks_sent)

    # Reserve the resources
    jobs = execo_g5k.oarsub(
        [
            (
                execo_g5k.OarSubmission(f"nodes={nb_reserved_nodes}", walltime=15 * 60),
                "luxembourg",
            )
        ]
    )
    job_id, site = jobs[0]

    # Get useful stats
    nodes = execo_g5k.get_oar_job_nodes(job_id, site)
    head_node, nodes = nodes[0], nodes[1:]

    print(head_node, nodes)

    cores_per_node = execo_g5k.get_host_attributes(head_node)["architecture"]["nb_cores"]
    total_simulation_cores = cores_per_node * len(nodes)

    print("\tSimulation cores available: ", total_simulation_cores)

    # Start the head node
    head_node_cmd = execo.SshProcess(
        'singularity exec doreisa/docker/images/doreisa-analytics.sif bash -c "cd doreisa; ray start --head --port=4242; python3 experiments/refs-gathering-benchmark/head.py"',
        head_node,
    )
    head_node_cmd.start()

    time.sleep(5)

    print("\tHead node started")

    # Start the simulation nodes
    for node in nodes:
        node_cmd = execo.SshProcess(
            f"""singularity exec doreisa/docker/images/doreisa-simulation.sif bash -c "ray start --address='{head_node.address}:4242'"; sleep infinity """,
            node,
        )
        node_cmd.start()

    # Write the hostfile
    with open("hostfile", "w") as hostfile:
        for node in nodes:
            for _ in range(cores_per_node):
                hostfile.write(f"{node.address}\n")

    # Wait for everything to start
    time.sleep(10)

    # Run the simulation
    cmd_simulation = execo.SshProcess(
        f"""bash -c "cd doreisa && singularity exec docker/images/doreisa-simulation.sif mpirun -n {nb_workers} --oversubscribe --hostfile hostfile singularity exec docker/images/doreisa-simulation.sif python3 experiments/refs-gathering-benchmark/worker.py {nb_chunks_sent}" """,
        head_node,
    )
    cmd_simulation.start()
    cmd_simulation.wait()

    # Release the ressources
    execo_g5k.oardel(jobs)


# Demonstrate the bottleneck when sending the references one by one.
# The results should be the same with 2 and 4 simulation nodes: the head node is the bottleneck.
for i in range(10):
    for j in range(10):
        if i + j > 12:
            continue

        # A head and 2 simulation nodes
        run_experiment(1 + 2, 2 ** i, 2 ** j)

        # A head and 4 simulation nodes
        run_experiment(1 + 4, 2 ** i, 2 ** j)
