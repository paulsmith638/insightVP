import sys,os,time,datetime,pickle
import numpy as np
#graphics imports
from plotly.subplots import make_subplots
import plotly.graph_objects as go
import webbrowser    



class Plotter():
    def __init__(self,chrome_path):
        webbrowser.register('chrome', None,webbrowser.BackgroundBrowser(chrome_path),1)

    def make_plot(self,panel_dlist,master_title="Data Plot"):
        n_plots = len(panel_dlist)
        group_names = list(pdict["name"] for pdict in panel_dlist)
        fig = make_subplots(rows=n_plots,cols=1,subplot_titles=group_names)
        for pi,pdict in enumerate(panel_dlist):
            slist = pdict["series"]
            for si,series in enumerate(slist):
                sname = series["name"]
                sdstr = series["dstr"]
                sval = series["val"]
                if si == 0:
                    fig.add_trace(go.Scatter(x=sdstr,y=sval,mode='lines',name=sname),row=pi+1,col=1)
                else:
                    fig.append_trace(go.Scatter(x=sdstr,y=sval,mode='lines',name=sname),row=pi+1,col=1)
        fig.update_layout(height=500*n_plots, width=800, title_text=master_title)
        return fig


    def show_plot(self,fig):
        fig.show(renderer="chrome")

    def tsarray_to_plot(self,tsa,vr_list,plot_name):
        #vr is list of tuples = (view,row)
        plot_dict = {}
        plot_dict["name"] = plot_name
        plot_dict["series"] = []
        dstr = tsa.dstr
        for view,row_name in vr_list:
            source = tsa.arrays[view]
            data = list(source[tsa.p2r[row_name]])
            name = " ".join([str(view),str(row_name)])
            sdict1 = {"name":name,"dstr":dstr,"val":data}
            plot_dict["series"].append(sdict1)
        return plot_dict

    
