source /root/.cache/pypoetry/virtualenvs/dask-on-ray-test-POrKbbXz-py3.12/bin/activate

mpic++ main.cpp -Wl,--copy-dt-needed-entries -lpdi -o simulation
pdirun mpirun -n 9 --oversubscribe --allow-run-as-root ./simulation

Start the head node:

```bash
ray start --head --port=4242 --include-dashboard=True
```

python3 head.py &


Compile Docker:
sudo docker build --pull --rm -f 'Dockerfile' -t 'daskonray:latest' '.' 

Run Docker:
sudo docker run --rm -it -v "$(pwd)":/workspace -w /workspace 'daskonray:latest' /bin/bash


poetry install --no-interaction --no-ansi --no-root