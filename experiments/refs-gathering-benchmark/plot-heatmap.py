import matplotlib.colors as colors
import matplotlib.pyplot as plt
import numpy as np

data = {}

with open("results-nancy-gros.txt", "r") as f:
    for line in f:
        line = line.strip()

        left, right = line.split(":")
        nb_machines, nb_processes, nb_refs_per_process = [int(x) for x in left.split()]
        time = float(right.strip())

        if nb_machines not in data:
            data[nb_machines] = []
        data[nb_machines].append((nb_processes, nb_refs_per_process, time))

# Compute the relative difference between the two experiments
# max_diff = 0

# for (p1, r1, t1), (p2, r2, t2) in zip(data[3], data[5]):
#     assert p1 == p2 and r1 == r2
#     max_diff = max(max_diff, abs(t1 - t2) / t1)

# print(f"Max diff: {max_diff:.2%}")

for a, values in data.items():
    ps = sorted(set(p for p, r, t in values))
    rs = sorted(set(r for p, r, t in values))

    grid = np.full((len(rs), len(ps)), np.nan)

    for p, r, t in values:
        i = rs.index(r)
        j = ps.index(p)
        grid[i, j] = t / (p * r)

    fig, ax = plt.subplots()

    ax.imshow(grid, aspect="auto", origin="lower", norm=colors.LogNorm())

    cbar = ax.figure.colorbar(ax.images[0], ax=ax)
    cbar.set_label("Time per iteration per reference (ms)")
    plt.xticks(np.arange(len(ps)), [str(int(p)) for p in ps])
    plt.yticks(np.arange(len(rs)), [str(int(r)) for r in rs])
    plt.xlabel("Number of processes")
    plt.ylabel("Number of references sent by process")
    plt.tight_layout()

    plt.savefig(f"heatmap_{a}.png")

    for (i, j), val in np.ndenumerate(grid):
        ax.text(j, i, f"{val:.2f}", ha="center", va="center", color="white")

    plt.savefig(f"heatmap_{a}_annotated.png")
