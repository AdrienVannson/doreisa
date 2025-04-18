import execo
import execo_g5k
import time
import threading


def run_experiment(nb_reserved_nodes: int, head_file: str) -> None:
    """
    Params:
        nb_reserved_nodes: The number of nodes to reserve on the Grid'5000 platform
        nb_chunks_sent: The number of chunks to send to the head node by each worker at each iteration
    """

    # Reserve the resources
    print(f"Asking for {nb_reserved_nodes} nodes...")

    jobs = execo_g5k.oarsub(
        [
            (
                execo_g5k.OarSubmission(f"nodes={nb_reserved_nodes}", walltime=15 * 60),
                "nancy",
            )
        ]
    )

    job_id, site = jobs[0]

    # Get useful stats
    nodes = execo_g5k.get_oar_job_nodes(job_id, site, timeout=None)
    head_node, nodes = nodes[0], nodes[1:]

    print(head_node, nodes, flush=True)

    nb_workers = 0
    for node in nodes:
        nb_workers += execo_g5k.get_host_attributes(node)["architecture"]["nb_cores"]

    print("Number of workers: ", nb_workers)

    # Stop ray everywhere
    stops = []
    for node in nodes + [head_node]:
        stop_ray = execo.SshProcess(
            """singularity exec doreisa/docker/images/doreisa-simulation.sif bash -c "ray stop" """,
            node,
        )
        stop_ray.start()
        stops.append(stop_ray)

    for stop in stops:
        stop.wait()

    # Start the head node
    # It is important to set the ulimit to a high value, otherwise, the simulation will be stuck
    head_node_cmd = execo.SshProcess(
        f'ulimit -n 65535; singularity exec doreisa/docker/images/doreisa-simulation.sif bash -c "cd doreisa; ray start --head --port=4242; python3 experiments/full-experiment/{head_file}"',
        head_node,
    )
    head_node_cmd.start()

    time.sleep(5)

    print("Head node started")

    # Start the simulation nodes
    for node in nodes:
        print("Starting node ", node, flush=True)
        node_cmd = execo.SshProcess(
            f"""ulimit -n 65535; singularity exec doreisa/docker/images/doreisa-simulation.sif bash -c "ray start --address='{head_node.address}:4242'"; sleep infinity """,
            node,
        )
        node_cmd.start()

    # Wait for everything to start
    time.sleep(10)

    print("Starting the simulation")

    # Run the simulation
    workers = []

    rank = 0

    for node in nodes:
        for _ in range(execo_g5k.get_host_attributes(node)["architecture"]["nb_cores"]):
            worker = execo.SshProcess(
                f"""ulimit -n 65535; singularity exec doreisa/docker/images/doreisa-simulation.sif python3 doreisa/experiments/full-experiment/worker.py {rank} {nb_workers}""",
                node,
            )
            worker.start()
            workers.append(worker)

            rank += 1

    print("All workers started")

    for worker in workers:
        worker.wait()

    # Release the ressources
    execo_g5k.oardel(jobs)


threads = []

for i in range(5, 7):
    t1 = threading.Thread(target=run_experiment, args=(2**i + 1, "head-small.py"))
    t2 = threading.Thread(target=run_experiment, args=(2**i + 1, "head.py"))
    threads.extend([t1, t2])
    t1.start()
    t2.start()

for t in threads:
    t.join()
