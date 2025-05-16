"""
Run an experiment to dertermine the maximum number of references that a single node can process.

This script must be executed from the root of the repository.
"""

import threading
import time

import execo
import execo_g5k


def run_experiment(nb_reserved_nodes: int, nb_workers: int, nb_chunks_sent: int) -> None:
    """
    Params:
        nb_reserved_nodes: The number of nodes to reserve on the Grid'5000 platform
        nb_workers: The number of MPI processes to use
        nb_chunks_sent: The number of chunks to send to the head node by each worker at each iteration
    """

    print("Starting experiment with", nb_reserved_nodes, nb_workers, nb_chunks_sent)

    # Reserve the resources
    jobs = execo_g5k.oarsub(
        [
            (
                execo_g5k.OarSubmission(resources=f"{{cluster='gros'}}/nodes={nb_reserved_nodes}", walltime=6 * 60),
                "nancy",
            )
        ]
    )
    job_id, site = jobs[0]

    # Get the nodes to use
    nodes = execo_g5k.get_oar_job_nodes(job_id, site)
    head_node, nodes = nodes[0], nodes[1:]

    print(head_node, nodes)

    # Start the head node
    head_node_cmd = execo.SshProcess(
        'ulimit -n 65535; singularity exec doreisa/docker/images/doreisa-simulation.sif bash -c "cd doreisa; ray start --head --port=4242; python3 experiments/refs-gathering-benchmark/head.py"',
        head_node,
    )
    head_node_cmd.start()

    time.sleep(5)

    print("Head node started")

    # Start the simulation nodes
    for node in nodes:
        node_cmd = execo.SshProcess(
            f"""ulimit -n 65535; singularity exec doreisa/docker/images/doreisa-simulation.sif bash -c "ray start --address='{head_node.address}:4242'; sleep infinity" """,
            node,
        )
        node_cmd.start()

    # Wait for everything to start
    time.sleep(10)

    workers = []

    while len(workers) < nb_workers:
        node = nodes[len(workers) % len(nodes)]

        worker = execo.SshProcess(
            f"ulimit -n 65535; cd doreisa && singularity exec docker/images/doreisa-simulation.sif python3 experiments/refs-gathering-benchmark/worker.py {len(workers)} {nb_workers} {nb_chunks_sent}",
            node,
        )
        worker.start()
        workers.append(worker)

    for worker in workers:
        worker.wait()

    # Release the ressources
    execo_g5k.oardel(jobs)


threads = []

# Demonstrate the bottleneck when sending the references one by one.
# The results should be the same with 2 and 4 simulation nodes: the head node is the bottleneck.
for i in range(10):
    for j in range(10):
        if i + j > 12:
            continue

        # A head and 4 simulation nodes
        threads.append(threading.Thread(target=run_experiment, args=(1 + 4, 2**i, 2**j)))
        threads[-1].start()

for t in threads:
    t.join()
