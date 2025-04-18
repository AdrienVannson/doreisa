import execo
import execo_g5k
import execo_engine
import time

JOB_ID, SITE = ..., ...


def run_experiment(nb_actors: int, nb_iterations: int) -> None:
    # Get useful stats
    nodes = execo_g5k.get_oar_job_nodes(JOB_ID, SITE, timeout=None)
    head_node, nodes = nodes[0], nodes[1:]

    print(head_node, nodes, flush=True)

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
        f'ulimit -n 65535; singularity exec doreisa/docker/images/doreisa-simulation.sif bash -c "cd doreisa; ray start --head --port=4242; python3 experiments/ray-benchmark/head.py {nb_actors} {nb_iterations}"',
        head_node,
    )
    head_node_cmd.start()

    time.sleep(2)

    # Start the worker nodes
    for node in nodes:
        print("Starting node ", node, flush=True)
        node_cmd = execo.SshProcess(
            f"""ulimit -n 65535; singularity exec doreisa/docker/images/doreisa-simulation.sif bash -c "ray start --address='{head_node.address}:4242'"; sleep infinity """,
            node,
        )
        node_cmd.start()

    head_node_cmd.wait()

for nb_actors in [1, 2, 4, 8, 16]:
    for nb_iterations in [1024, 2048, 4096, 8192, 16384, 32768, 65536]:
        run_experiment(nb_actors, nb_iterations)
