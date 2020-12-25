
# Standard Library imports
from datetime import datetime
import pandas as pd
import numpy as np
from pprint import pprint as pp
import statistics
import sys
import time

# Third party imports
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import matplotlib.dates as dates

# Load Application imports
from dmyplant.dReliability import demonstrated_reliability_sr


def idx(n, s, e, x):
    return int(n * (x - s) / (e - s) + 1)


def demonstrated_Reliabillity_Plot(vl, beta=1.21, T=30000, s=1000, ft=pd.DataFrame, cl=[10, 50, 90], xmin=None, xmax=None, factor=2.0):

    # define milestones
    start_ts = vl.valstart_ts if xmin == None else xmin  # val start

    # the end of the Plotting interval
    if xmax:
        last_ts = xmax
    else:
        if factor:
            factor = max(factor, 1.05)  # no factor < 1.0 allowed
            elapsed = vl.now_ts - start_ts
            last_ts = start_ts + factor * elapsed
        else:
            raise ValueError("No timerange specified.")

    fcol = 'grey'

    # first calculate the x axis timerange
    tr = demonstrated_reliability_sr(vl,
                                     start_ts, last_ts, beta=beta, size=s, ft=ft)[0]  # timestamp x axis start .. end

    # determine the array - index of 'now'
    n_i = idx(s, start_ts, last_ts, vl.now_ts)

    # create Timerow from Start to 'now'
    n_tr = tr[0:n_i:1]

    # convert to datetime dates - start .. last
    dtr = [datetime.fromtimestamp(t) for t in tr]
    # calculate demonstrated reliability curves for the complete period,
    # confidence intervals CL :
    rel = {c: demonstrated_reliability_sr(vl, start_ts, last_ts,
                                          CL=c/100.0, beta=beta, size=s, ft=ft, T=T)[1] for c in cl}

    # convert to datetime dates - start .. now
    n_dtr = [datetime.fromtimestamp(t) for t in n_tr]
    # copy demontrated reliability values for the validation period up to now:
    n_rel = {c: rel[c][0:n_i:1] for c in cl}

    # define the PLOT
    fig, ax1 = plt.subplots(  # pylint: disable=unused-variable
        figsize=(12, 8), constrained_layout=True)
    #fig, (ax1, ax3) = plt.subplots(2, figsize=(6, 6))

    color = 'tab:red'
    ax1.set_xlabel('date')
    ax1.set_ylabel('Demonstrated Reliability [%]', color=color)
    ax1.set_title('Demonstrated Reliability [%]')

    # now plot the demonstrated reliability curves:
    for CL in cl:
        # complete interval in color fcal
        ax1.plot(dtr, rel[CL], color=fcol, linestyle='-', linewidth=0.5)
        # the current validation interval in multiple colors
        ax1.plot(n_dtr, n_rel[CL], color='red', linestyle='-', linewidth=0.7)

    # define the axis ticks
    ax1.tick_params(axis='y', labelcolor=color)

    # and the axis scales
    ax1.axis((datetime.fromtimestamp(start_ts),
              datetime.fromtimestamp(last_ts), 0, 100))

    # define axis intervals y ...
    ax1.yaxis.set_major_locator(ticker.LinearLocator(13))

    # and x - axis
    locator = dates.AutoDateLocator()
    locator.intervald[dates.MONTHLY] = [1]
    ax1.xaxis.set_major_locator(locator)

    # show a grid
    ax1.grid(color='lightgrey')

    # instantiate a second axes that shares the same x-axis
    ax2 = ax1.twinx()

    # and plot the linearized engine runtime lines vs the 2nd axis
    for e in vl.engines[:]:
        #print(e.Name, e._d['Engine ID'], e._d['val start'], e._d['oph parts'])
        # complete interval in color fcal
        y = [e.oph(t) for t in tr]
        ax2.plot(dtr, y, linewidth=0.5, color=fcol)
        # the current validation interval in multiple colors
        n_y = [e.oph(t) for t in n_tr]
        ax2.plot(n_dtr, n_y)

    # NOW plot some Orientation Lines and Test into the Plot

    # possible runtime line
    y = [(t-start_ts) / 3600.0 for t in tr]
    ax2.plot(dtr, y, color='grey', linestyle='--', linewidth=0.7)

    # today line
    ax1.axvline(datetime.now(), color='red',
                linestyle='--', linewidth=0.7)

    # Point of demonstrated reliability at
    # highest Confiedence Level, today
    myrel_y = float(
        rel[max(cl)][int((vl.now_ts-start_ts)/(last_ts - start_ts)*s)])
    myrel_x = datetime.fromtimestamp(vl.now_ts)
    ax1.scatter(myrel_x, myrel_y, marker='o', color='black', label='point')
    txt = f"CL {max(cl)}%@{T}\nbeta={beta}\nR={myrel_y:.1f}%"

    # some statistical Information.
    myrel_txt_x = datetime.fromtimestamp(vl.now_ts + 200000)
    ax1.text(myrel_txt_x, myrel_y - 9, txt)
    ax1.axis((datetime.fromtimestamp(start_ts),
              datetime.fromtimestamp(last_ts), 0, 120))
    # oph Fleet Leader
    fl = [e.oph(vl.now_ts) for e in vl.engines]
    fl_point_x = datetime.fromtimestamp(vl.now_ts)
    ax2.scatter(fl_point_x, max(fl), marker='o', color='black', label='point')
    fl_txt_x = datetime.fromtimestamp(vl.now_ts + 200000)
    txt = f'{len(fl)} engines\nmax {max(fl):.0f}h\ncum {sum(fl):.0f}h\navg {statistics.mean(fl):.0f}h\n{datetime.now():%d.%m.%Y %H:%M}'
    ax2.text(fl_txt_x, max(fl) - T/7, txt)
    ax2.axis((datetime.fromtimestamp(start_ts),
              datetime.fromtimestamp(last_ts), 0, 24000))
    color = 'tab:blue'
    # the x-label was handled with ax1
    ax2.set_ylabel('hours [h]', color=color)
    ax2.tick_params(axis='y', labelcolor=color)
    ax2.yaxis.set_major_locator(ticker.LinearLocator(13))

    # TATAAAAH!
    plt.show()


if __name__ == '__main__':
    pass
