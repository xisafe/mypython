import numpy as np
from numpy.random import randn
import matplotlib.pyplot as plt 
from matplotlib import gridspec
def to_percent(y, position):
    s = str(100 * y)
    if matplotlib.rcParams['text.usetex'] is True:
        return s + r'$\%$'
    else:
        return s + '%' 
# generate some data
x=valid_data[valid_data['weight_or_vol']==True]['diff_rate']
y = np.sin(x)

def plotDist(X):
    print("sidnsdss是多少")
    formatter = FuncFormatter(to_percent)
    plt.figure(figsize=(10, 7)) 
    gs = gridspec.GridSpec(2, 1, height_ratios=[1,12]) 
    ax0 = plt.subplot(gs[0])
    ax0.boxplot(X,vert=False)
    plt.grid()
    ax1 = plt.subplot(gs[1])
    ax1.set_ylabel('diff_rate distribution')
    ax1.hist(X, bins=40, normed=True,color='g')
    plt.gca().yaxis.set_major_formatter(formatter)
    plt.grid()
    ax2 = ax1.twinx()  # this is the important function
    ax2.set_ylabel('diff_rate cumulative distribution')
    plt.gca().yaxis.set_major_formatter(formatter)
    ax2.hist(X, bins=40, normed=True,color='red',cumulative=True,histtype='step')
    plt.annotate('Mean : '+format(X.mean(),'.2f')+'', xy=(-90, 1), xytext=(-90, 1))
    plt.annotate(' Std :' + format(X.std(),'.2f')+'', xy=(-90, 1), xytext=(-90, 0.95))
    plt.annotate(' Var :' + format(X.var(),'.2f')+'', xy=(-90, 1), xytext=(-90, 0.90))
    plt.tight_layout()
    plt.show()
    plt.close()
    
X=x  
formatter = FuncFormatter(to_percent)
plt.figure(figsize=(10, 7)) 
gs = gridspec.GridSpec(2, 1, height_ratios=[1,12]) 
ax0 = plt.subplot(gs[0])
ax0.boxplot(X,vert=False)
plt.grid()
ax1 = plt.subplot(gs[1])
ax1.set_ylabel('diff_rate distribution')
ax1.hist(X, bins=40, normed=True,color='g')
plt.gca().yaxis.set_major_formatter(formatter)
plt.grid()
ax2 = ax1.twinx()  # this is the important function
ax2.set_ylabel('diff_rate cumulative distribution')
plt.gca().yaxis.set_major_formatter(formatter)
ax2.hist(X, bins=40, normed=True,color='red',cumulative=True,histtype='step')
plt.annotate('Mean : '+format(X.mean(),'.2f')+'', xy=(-90, 1), xytext=(-90, 1))
plt.annotate(' Std :' + format(X.std(),'.2f')+'', xy=(-90, 1), xytext=(-90, 0.95))
plt.annotate(' Var :' + format(X.var(),'.2f')+'', xy=(-90, 1), xytext=(-90, 0.90))
plt.tight_layout()
plt.show()
plt.close()