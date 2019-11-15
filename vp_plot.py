import sys,os,time,datetime,pickle
import numpy as np
#graphics imports
from plotly.subplots import make_subplots
import plotly.graph_objects as go
import webbrowser    


CHROME_PATH = "/opt/google/chrome/chrome"

class Plotter():
    def __init__(self,chrome_path=None):
        if chrome_path is None:
            chrome_path = CHROME_PATH
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
                mode = series.get("mode","lines") #default line mode
                if mode not in ("lines","markers"):
                    mode='lines'
                if si == 0:
                    fig.add_trace(go.Scatter(x=sdstr,y=sval,mode=mode,name=sname),row=pi+1,col=1)
                else:
                    fig.append_trace(go.Scatter(x=sdstr,y=sval,mode=mode,name=sname),row=pi+1,col=1)
                if mode == "markers":
                    fig.update_traces(marker_size=20)
        fig.update_layout(height=500*n_plots, width=800, title_text=master_title)
        return fig


    def show_plot(self,fig):
        fig.show(renderer="chrome")

    def tsarray_to_plot(self,tsa,vr_list,plot_name,start=None,end=None):
        #vr is list of tuples = (view,row)
        plot_dict = {}
        plot_dict["name"] = plot_name
        plot_dict["series"] = []
        dstr = tsa.dstr
        valid_index=None
        if start is not None and end is not None:
            all_dt = tsa.dt_list
            start_dt = datetime.datetime.strptime(start,"%Y-%m-%d")
            end_dt = datetime.datetime.strptime(end,"%Y-%m-%d")
            valid_index = list(i for i in range(len(dstr)) if all_dt[i] >= start_dt and all_dt[i] <= end_dt)
        for view,row_name in vr_list:
            source = tsa.arrays[view]
            data = list(source[tsa.p2r[row_name]])
            if valid_index is not None:
                data = list(data[i] for i in valid_index)
                dstr_out = list(dstr[i] for i in valid_index)
            else:
                dstr_out = dstr
            name = " ".join([str(view),str(row_name)])
            sdict1 = {"name":name,"dstr":dstr_out,"val":data}
            plot_dict["series"].append(sdict1)
        return plot_dict

    def plot_frames_series(self,tsa,grouping="byframe",frame_list=None,series_list=None,start=None,end=None,title="Graphs"):
        if series_list is None:
            series_list = tsa.p2r.keys()
        if frame_list is None:
            frame_list = tsa.arrays.keys()
        if start is None:
            start = tsa.dstr[0]
        if end is None:
            end = tsa.dstr[-1]
        plot_dlist = []
        if grouping == "byframe":
            for frame in frame_list:
                to_plot = list((frame,series) for series in series_list)
                plot_dict = self.tsarray_to_plot(tsa,to_plot,str(frame),start=start,end=end)
                plot_dlist.append(plot_dict)
            return plot_dlist
        elif grouping  == "byseries":
            for series in series_list:
                to_plot = list((frame,series) for frame in frame_list)
                plot_dict = self.tsarray_to_plot(tsa,to_plot,str(series),start=start,end=end)
                plot_dlist.append(plot_dict)
            return plot_dlist
        else:
            print "grouping %s not understood!  Aborting" % grouping
            return []
    

    def get_outlier_plot(self,tsa,source_array,outlier_array,name_lookup=None):
        data = tsa.arrays[source_array]
        outlier_data = tsa.arrays[outlier_array]
        outlier_mask = outlier_data > 0.1
        outlier_rows = np.nansum(outlier_mask,axis=1) > 0
        outlier_ri=list(np.nonzero(outlier_rows)[0])
        panel_dlist = []
        for ri in outlier_ri:
            rname = tsa.r2p[ri]
            if name_lookup is not None:
                name_append=name_lookup.get(rname,"Unknown")
            else:
                name_append=""
            title="Outlier for "+str(rname)+" "+str(name_append)
            outlier_pdict = {}
            outlier_pdict["name"] = title
            rdata = list(data[ri])
            outlier_ci = list(np.nonzero(outlier_mask[ri])[0])
            outlier_dstr = list(tsa.dstr[index] for index in outlier_ci)
            outlier_val = list(rdata[index] for index in outlier_ci)
            outlier_sub = list(outlier_data[ri,index] for index in outlier_ci)
            sdict1 = {"name":str(rname),"dstr":tsa.dstr,"val":rdata}
            sdict2 = {"name":str(rname)+"_out","dstr":outlier_dstr,"val":outlier_val,"mode":"markers"}
            sdict3 = {"name":str(rname)+"_new","dstr":outlier_dstr,"val":outlier_sub,"mode":"markers"}
            outlier_pdict["series"] = [sdict1,sdict2,sdict3]
            panel_dlist.append(outlier_pdict)
        return panel_dlist

    
