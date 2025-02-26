source /root/.cache/pypoetry/virtualenvs/dask-on-ray-test-POrKbbXz-py3.12/bin/activate

mpic++ main.cpp -Wl,--copy-dt-needed-entries -lpdi -o simulation
pdirun mpirun -n 9 --oversubscribe --allow-run-as-root ./simulation

Start the head node:

```bash
ray start --head --port=4242 --include-dashboard=True
```

python3 head.py &

Build Docker:
podman build --pull --rm -f 'docker/simulation/Dockerfile' -t 'doreisa_simulation:latest' 'docker/simulation'

Run Docker:
docker run --rm -it --shm-size=2gb -v "$(pwd)":/workspace -w /workspace 'doreisa_simulation:latest' /bin/bash


poetry install --no-interaction --no-ansi --no-root