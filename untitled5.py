import matplotlib
from numpy.random import randn
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter


def to_percent(y, position):
    # Ignore the passed in position. This has the effect of scaling the default
    # tick locations.
    s = str(100 * y)

    # The percent symbol needs escaping in latex
    if matplotlib.rcParams['text.usetex'] is True:
        return s + r'$\%$'
    else:
        return s + '%'

x = randn(5000)

# Make a normed histogram. It'll be multiplied by 100 later.


# Create the formatter using the function to_percent. This multiplies all the
# default labels by 100, making them all percentages
plt.hist(x, bins=50, normed=True)
formatter = FuncFormatter(to_percent)
plt.gca().yaxis.set_major_formatter(formatter)
plt.show()
# Set the formatter

import matplotlib.pyplot as plt
import numpy as np
x = np.arange(0., np.e, 0.01)
y1 = np.exp(-x)
y2 = np.log(x)
fig = plt.figure()
ax1 = fig.add_subplot(111)
ax1.plot(x, y1)
ax1.set_ylabel('Y values for exp(-x)')
ax1.set_title("Double Y axis")
ax2 = ax1.twinx()  # this is the important function
ax2.plot(x, y2, 'r')
ax2.set_xlim([0, np.e])
ax2.set_ylabel('Y values for ln(x)')
ax2.set_xlabel('Same X for both exp(-x) and ln(x)')
plt.show()
plt.close()
import matplotlib.pyplot as plt

nx = 1
ny = 4

dxs = 8.0
dys = 6.0

fig, ax = plt.subplots(ny, nx, sharey = True)
for i in range(nx):
    ax[i].plot([1, 2], [1, 2])