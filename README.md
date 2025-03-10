# Dask-on-Ray Enabled In Situ Analytics

## Installation

### Using containers

Doreisa can be install using containers: a Docker image is built. This image can then be used with singularity.

On Grid5000, first, enable Docker with `g5k-setup-docker -t`. This is only needed to build the images, not to execute the code.

Execute the building script: `$ ./build-images.sh`. This will build the Docker images, save them to a tar file and convert them to singularity images.

## Notes (TODEL)

mpic++ main.cpp -Wl,--copy-dt-needed-entries -lpdi -o simulation
pdirun mpirun -n 9 --oversubscribe --allow-run-as-root ./simulation

Start the head node:

```bash
ray start --head --port=4242 --include-dashboard=True
```

python3 head.py

Build Docker:
podman build --pull --rm -f 'docker/simulation/Dockerfile' -t 'doreisa_simulation:latest' 'docker/simulation'

docker build --pull --rm -f 'Dockerfile' -t 'doreisa:latest' '.'
docker save doreisa:latest -o doreisa-image.tar
singularity build ./doreisa.sif docker-archive://doreisa-image.tar
singularity build ./doreisa.sif docker-archive://doreisa-image.tar
mpirun -n 3 singularity exec ./doreisa.sif hostname

If needed: singularity shell

Run Podman:
podman run --rm -it --shm-size=2gb --network host -p 8000:8000 -v "$(pwd)":/workspace -w /workspace 'doreisa_simulation:latest' /bin/bash

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
 - The analytics might want to do a convolution with a small kernel. In this case, we want to avoid sending all the data. Measure this
 - See if Infiniband is not supported in Ray


!! Prepare a presentation about the work for now -> demo

Doreisa


mpirun -machinefile $OAR_NODEFILE singularity exec ./doreisa.sif hostname
mpirun -machinefile $OAR_NODEFILE singularity exec ./doreisa.sif 