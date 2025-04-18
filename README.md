# Dask-on-Ray Enabled In Situ Analytics

## Installation

### Using containers

Doreisa can be install using containers: a Docker image is built. This image can then be used with singularity.

On Grid5000, first, enable Docker with `g5k-setup-docker -t`. This is only needed to build the images, not to execute the code.

Execute the building script: `$ ./build-images.sh`. This will build the Docker images, save them to tar files and convert them to singularity images.

## Developement

### Update the Python environment

To add dependencies to the Python environment, add them via poetry. Then, export them to `requirements.txt` via:

```
poetry export -f requirements.txt --output requirements.txt
```

This file should be copied in `docker/analytics/` and `docker/simulation`. Remove numpy from the file in `docker/simulation` since another version is already installed with PDI.

## Notes (TODEL)

mpic++ main.cpp -Wl,--copy-dt-needed-entries -lpdi -o simulation
pdirun mpirun -n 9 --oversubscribe --allow-run-as-root ./simulation

Start the head node:

```bash
ray start --head --port=4242 --include-dashboard=True
```

python3 head.py

mpirun -n 3 singularity exec ./doreisa.sif hostname

If needed: singularity shell

Run Podman:
podman run --rm -it --shm-size=2gb --network host -v "$(pwd)":/workspace -w /workspace 'doreisa-simulation:latest' /bin/bash

Run Docker:
docker run --rm -it --shm-size=2gb -v "$(pwd)":/workspace -w /workspace 'doreisa_simulation:latest' /bin/bash


poetry install --no-interaction --no-ansi --no-root

## TODO

 - Examples of analytics (time derivative)
 - Don't block the simulation code. Send the data and keep going
 - Do some analytics at certain timesteps only, in case of specific events.
    Example: if the temperature becomes too high, perform the analyics more often (every 10 steps instead of every 100 steps)
    For parflow, the silulation is performed every dt, but dt can vary accross the simulation
 - Support two scenarios:
    - Simulation running on GPU -> can perform the computation in situ, on the same node
    - Simulation running on CPU -> should send the data right away, process in transfer
    Let the user choose if the chunks are stored on the same node, or in another node
    Using ray placement groups?
    Dynamically to avoid being out of memory?

    We should be able, from to client, to choose a function to execute on the numpy array as soon as available. For example, compute an integral without copying the data, and then sending only the required data.

 - The analytics might want to do a convolution with a small kernel. In this case, we want to avoid sending all the data. Measure this
 - See if Infiniband is not supported in Ray

 - PDI makes a copy only when the data is on the GPU

 - Adastra (Ruche)?

 - Contract: choose which piece of data are needed. We might not want all the available arrays -> don't make a copy in that case. For example, only do the `ray.put` every 100 iterations

 - Would be nice to estimate the CO2 emission (if large scale experiment)

!! Prepare a presentation about the work for now -> demo

Doreisa


mpirun -machinefile $OAR_NODEFILE singularity exec ./doreisa.sif hostname
mpirun -machinefile $OAR_NODEFILE singularity exec ./doreisa.sif



ZMQ to make remote copies of numpy array



present windw approach as research

Simulation : eulerian vs lagrangian vs semi-lagrangian

Understand the ray scheduling strategy

Same for dask on ray

1. Scalability benchmark (!)
2. In-situ / in-transfer API
3. Feedback loop (for the simulation and analytics)
4. Dask on Ray
5. Scheduling: Dask, Ray, Dask-on-Ray -> understand better (!)
6. Slicing, avoid full object moves (ex: convolutions)
