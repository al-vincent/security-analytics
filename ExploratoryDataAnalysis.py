# -*- coding: utf-8 -*-
#
#Created on Thu Oct  5 08:19:09 2017
#
#@author: AVINCENT

# TODO
#Possible features: 
#------------------
#1. Total traffic exchanged across whole network per hour, per day
#2. Volume (and proportion?) of total traffic for each port / service / application
#3. Volume (and proportion?) of traffic going outside (i.e. to IPs other than 192.168.x.x)
#4. Volume (and proportion?) of traffic going to / from each (internal) IP

import pandas as pd
import matplotlib.pyplot as plt
#from matplotlib.font_manager import FontProperties
from sys import exit

class Constants():
    def __init__(self):
        self.client = 'client'
        self.server = 'server'
        self.c_bytes = 'client_bytes'
        self.s_bytes = 'server_bytes'
        self.start = 'start'
        self.stop = 'stop'
                
        self.t_bytes = 'total_bytes'
        self.is_ext = 'is_outside'
        self.date = 'date'

class ProcessFlowData():
    def __init__(self, f_in):
        self.df = self.load_data(f_in)
        
    def load_data(self,f_in):
        try:
            return pd.read_csv(f_in)
        except FileNotFoundError:
            print("File " + f_in + " could not be found")
            exit(1)
    
    def process_data(self):
        C = Constants()
        # -------------------------------------------------------------------------
        # Process data
        # -------------------------------------------------------------------------
        # Get date when flow took place
        #self.df[C.date] = self.extract_dates()
        # Convert 'start' and 'stop' times to timestamps
        self.df[C.start] =  self.convert_timestamps(C.start)
        self.df[C.stop] =  self.convert_timestamps(C.stop)
        # Mark all traffic to a server outside the 192.168.x.x range as 'external'
        self.df[C.is_ext] = self.get_external_traffic()
        # calculate total number of bytes between each client and server pair
        self.df[C.t_bytes] = self.df[C.s_bytes] + self.df[C.c_bytes]
    
    def convert_timestamps(self, col):
        ts1 = pd.to_datetime(self.df[col], format="%H:%M:%S %d/%m/%y", 
                                 errors='coerce')
        ts2 = pd.to_datetime(self.df[col], format="%d/%m/%Y %H:%M", 
                                 errors='coerce')
        return ts1.fillna(ts2)
        
#    def extract_dates(self):
#        C = Constants()
#        # NOTE: for 21/10/16, the 'start' field is formatted as DD/MM/YYYY hh:mm,
#        # whereas for all other dates it's formatted as hh:mm:ss DD/MM/YY
#        dates1 = self.df[C.start].str.extract('(../../..)', expand=True)
#        dates2 = self.df[C.start].str.extract('(../../....)', expand=True)
#        return dates1.fillna(dates2)
    
#    def extract_times(self):
#        # get start and end *times* for flows
#        # NOTE: currently incorrect for 21/10/16
#        #    df[C.start] = df[C.start].str[0:8]
#        #    df[C.stop] = df[C.stop].str[0:8]   
#        pass
    
    def get_external_traffic(self):
        C = Constants()
        return self.df[C.server].str[0:7] != '192.168'
        
    def plot_total_bytes_per_client_per_day_bar(self, time):
        C = Constants()
        grp_day_client = self.df.groupby([C.date, C.client])
        client_byte_sums = grp_day_client[C.t_bytes].sum()
        days = client_byte_sums.index.get_level_values(0).unique()
        for day in days:            
            t = "Total bytes per client, " + day
            plt.figure(); client_byte_sums.xs(day).plot.bar(title=t)
    
    def plot_external_traffic_per_client_per_day(self):
        C = Constants()
        # subset the main dataset, to only get what's needed
        dat = self.df[self.df[C.is_ext]][[C.date, C.client,C.t_bytes]]
        # pivot this (cut-down) dataset into a useful form
        daily_ext_traffic = pd.pivot_table(dat, values = C.t_bytes,
                                           index=C.date, columns=C.client).fillna(0)
        
        # single graph showing total bytes of data sent to external servers
        # by each client, over time. Quite messy and hard to read :-s
        daily_ext_traffic.plot.line()        
        
        # Separate line graphs, each showing the total amount of data sent by a 
        # single client to any external server, per day. With small networks, 
        # this is ok; with large networks,there'll be *a lot* of graphs.
        clients = daily_ext_traffic.columns.values
        for client in clients:            
            t = "Total bytes of external traffic, client, " + client
            plt.figure(); daily_ext_traffic[client].plot.line(title=t)
    
    def plot_field_per_timeblock(self, field, time):
        C = Constants()
        subset = self.df[[C.start,field]].set_index(C.start)
        subset = subset.resample(time).sum()
        subset.plot.line()#.legend(loc='center left',bbox_to_anchor=(1.0, 0.5))
    
    def plot_percent_traffic_per_src_port_per_period(self, time):
        # 1. Get total traffic per src port (using pivot_table, as per below(?))
        # 2. Set 'start' to be key and resample based on 'time'
        # 3. Separately, get total traffic per day
        # 4. Use tot_traffic to calculate % of traffic per port
        # 5. Plot this as line graph, one line per port
        #    > N.B. this may end up with a RIDICULOUS number of lines, given 
        #      there are ~65,000 ports...
    
        pass
        
    def plot_field_per_timeblock_per_client(self, field, time):
        C = Constants()
        t = field +" per " + time + " window"

        df = pd.pivot_table(self.df, values=field, index=C.start, 
                            columns=C.client).fillna(0)        
        df = df.resample(time).sum()
        df.plot.line(title=t).legend(loc='center left',
                                     bbox_to_anchor=(1.0, 0.5),prop={'size':8})
    
    def process_flows(self):
        C= Constants()
        self.process_data()
        #self.plot_total_bytes_per_day_line()
        #self.plot_total_bytes_per_day_bar()
        #self.plot_external_traffic_per_day()
        self.plot_field_per_timeblock(C.t_bytes,"4H")
        self.plot_field_per_timeblock_per_client(C.t_bytes,"D")

def main():
    # set the input file    
    f_in = "../Data/all_output.csv"
    #f_in = "../Data/20160111.csv"
    # create a ProcessFlowData object
    pfd = ProcessFlowData(f_in)
    # process and display the data
    pfd.process_flows()

if __name__ == '__main__':
    main()
    
