from matplotlib import pyplot as plt
import matplotlib.ticker

data = """36 19.1283917427063 54.62046146392822
72 37.082438468933105 107.1160101890564
144 71.81367874145508 205.5645251274109
288 146.49715900421143 405.048725605011
576 300.103862285614 820.530059337616
1152 625.9033441543579 1676.0836482048035"""

xs, ys1, ys2 = [], [], []

for line in data.split("\n"):
    x, y1, y2 = map(float, line.split(" "))

    xs.append(x)
    ys1.append(y1)
    ys2.append(y2)

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(8, 4))

# Total time per iteration
ax1.set_xlabel("Number of workers")
ax1.set_ylabel("Time per iteration (ms)")

ax1.set_xscale("log")
ax1.set_yscale("log")

ax1.get_xaxis().set_major_formatter(matplotlib.ticker.ScalarFormatter())
ax1.get_xaxis().minorticks_off()
ax1.set_xticks(xs)

ax1.plot(xs, ys2)
ax1.scatter(xs, ys2)

# Proportions
ax2.set_xlabel("Number of workers")
ax2.set_ylabel("Proportion of the total time (%)")

ax2.set_xscale("log")

ax2.get_xaxis().set_major_formatter(matplotlib.ticker.ScalarFormatter())
ax2.get_xaxis().minorticks_off()
ax2.set_xticks(xs)


ratio = [y1 / y2 * 100 for y1, y2 in zip(ys1, ys2)]

ax2.fill_between(xs, ratio, 100, alpha=0.4, facecolor='orange', label="Scheduling the tasks")
ax2.fill_between(xs, 0, ratio, alpha=0.4, facecolor='red', label="Building the array")
ax2.legend(loc="upper right")

ax2.plot(xs, ratio)
ax2.scatter(xs, ratio)

plt.tight_layout()
plt.savefig("plot.png")
