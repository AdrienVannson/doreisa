# Dask-on-Ray Enabled In Situ Analytics

mpic++ main.cpp -Wl,--copy-dt-needed-entries -lpdi -o simulation
pdirun mpirun -n 9 --oversubscribe --allow-run-as-root ./simulation

Start the head node:

```bash
ray start --head --port=4242 --include-dashboard=True
```

python3 head.py &

Build Docker:
podman build --pull --rm -f 'docker/simulation/Dockerfile' -t 'doreisa_simulation:latest' 'docker/simulation'

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
 - The analytics might want to do a convolution with a small kernel. In this case, we want to avoid sending all the data
 - See if Infiniband is not supported in Ray


Doreisa