from utils.read_exp_utils import read_experiment_result
import numpy as np
import matplotlib.pyplot as plt
plt.switch_backend('agg')#plot without using an X-server, use the Agg backend instead.
from matplotlib.backends.backend_pdf import PdfPages
from sklearn.metrics import * 
import itertools
import os
import pandas as pd

class ParameterResult:
    def  __init__(self, experiment_id, model_id, parameters_id):
        '''
        Generate a report for each set of hyperparameters
        '''

        ## hit the result table and get all the val results for this
        ## experiment, model, parameter setting.
        self.results=read_experiment_result(experiment_id, model_id, parameters_id)
        #housekeeping variables
        self.experiment_id = experiment_id
        self.model_id = model_id
        self.parameters_id = parameters_id
        self.val_set_count = self.results.shape[0]
        prc=self.results['precision_recall_curve'][0]
        self.k_list  = [i for i in range(1,len(prc['recall_all'])+1)]
        self.k_count = len(self.k_list)#number of k

        #val_avg_threshold
        self.p_avg_array={}
        self.r_avg_array={}
        self.f1_avg_array={}
        self.p_avg_array['all'],self.r_avg_array['all'],self.f1_avg_array['all']=self._val_avg_by_k()
        self.p_avg_array['day'],self.r_avg_array['day'],self.f1_avg_array['day']=self._val_avg_by_k(time='day')
        self.p_avg_array['night'],self.r_avg_array['night'],self.f1_avg_array['night']=self._val_avg_by_k(time='night')

        #path to save report file
        self.report_path =  os.getcwd()

    def adjusted_pred(self,y_scores, t):
        """
        Return predictions based on a given threshold(t)
        """
        return [1 if y >= t else 0 for y in y_scores]        
        
    def f1_list(self,precisions:list,recalls:list,output="list"):
        '''
        Return a list of f1-scores in python list or numpy array format
        '''
        np_prec = np.asarray(precisions)
        np_rec = np.asarray(recalls)
        f1= (2.0*np_prec*np_rec) / (np_prec+np_rec)
        f1= np.nan_to_num(f1)
        max_f1_ix=f1.argmax(axis=0)
        if output=="list":
            return f1.tolist(), max_f1_ix
        elif output=="numpy":
            return f1, max_f1_ix
        else:
            raise Exception("please choose one of available outputs:\nlist: python list\nnumpy:numpy array")
                

    def _val_avg_by_k(self,time='all'):
        """Avg precision, recall, and f1-score (across all val sets) for each k"""
        p_array = np.empty([self.val_set_count, self.k_count])
        r_array = np.empty([self.val_set_count, self.k_count])
        f1_array = np.empty([self.val_set_count, self.k_count])

        for i, row in self.results.iterrows():
            prc=row.precision_recall_curve
            p_array[i] = prc[f'precision_{time}']
            r_array[i] = prc[f'recall_{time}']
            f1_array[i],_= self.f1_list(prc[f'precision_{time}'],prc[f'recall_{time}'],output="numpy")

        p_avg_array=np.average(p_array,axis=0)
        r_avg_array=np.average(r_array,axis=0)
        f1_avg_array=np.average(f1_array,axis=0)

        return p_avg_array,r_avg_array,f1_avg_array


    def plot_roc(self,name, results:list,pdf_report,output_type="save"):
        '''Plot roc curve based on all results from all validation sets'''
        plt.clf()
        for index, row in self.results.iterrows():
            plt.plot(row.roc['fpr'], row.roc['tpr'], label='Val set:%d ROC curve (area = %0.2f)' % (row.val_set_id,row.auc_roc))
        plt.plot([0, 1], [0, 1], 'k--')
        plt.xlim([0.0, 1.05])
        plt.ylim([0.0, 1.05])
        plt.xlabel('False Positive Rate')
        plt.ylabel('True Positive Rate')
        plt.title(name)
        plt.legend(loc="lower right")
        if (output_type == 'save'):
            pdf_report.savefig(bbox_inches='tight')
        elif (output_type == 'show'):
            plt.show()
        else:
            plt.show()

    def plot_precision_recall_n(self,model_name,pdf_report,time='all',plot_type="avg",output_type="save"):
        '''Plot precision_recall vs number of inspectors'''
        if plot_type == "avg":
            #Avg precision/recall vs number of inspectors
            precision_curve = self.p_avg_array[time]
            recall_curve = self.r_avg_array[time]

            #PLOT
            plt.clf()
            fig, ax1 = plt.subplots()
            ax1.plot(self.k_list, precision_curve, 'b')
            ax2 = ax1.twinx()
            ax2.plot(self.k_list, recall_curve, 'r')
        
        elif plot_type == "normal":
            plt.clf()
            fig, ax1 = plt.subplots()
            ax2 = ax1.twinx()
            for i, row in self.results.iterrows():
                precision_curve = row.precision_recall_curve[f'precision_{time}']
                recall_curve = row.precision_recall_curve[f'recall_{time}']
                #plot
                ax1.plot(self.k_list, precision_curve, 'b')
                ax2.plot(self.k_list, recall_curve, 'r')
        ax1.set_xlabel('number of road segments')
        ax1.set_ylabel('precision',color='b')
        ax2.set_ylabel('recall',color='r')
        ax1.set_ylim([0,1.03])#extra 0.3 just to make the plot looks nicer
        ax2.set_ylim([0,1.03])
        ax1.set_title(f'PR CURVE: {time} time ')

        name = model_name
        if (output_type == 'save'):
            pdf_report.savefig( bbox_inches='tight')
        elif (output_type == 'show'):
            plt.show()
        else:
            plt.show()
        
    def generate_report(self):
        '''generate a pdf report'''
        report_filepath = self.report_path+"/"+"-".join([str(self.experiment_id),self.model_id,self.parameters_id])+".pdf"
            
        print(self.model_id,self.parameters_id)
        print("f1_at8:" ,self.f1_avg_array['day'][7],\
              "p_at8:",self.p_avg_array['day'][7],\
              "r_at8:",self.r_avg_array['day'][7])
        #PLOT
        pdf_report = PdfPages(report_filepath) #initiate pdf_report
        #ROC
        roc_name = "-".join(["ROC",self.model_id,self.parameters_id])
        self.plot_roc(roc_name, self.results,pdf_report,output_type="save")
        #Precision Recall Population
        model_name="-".join(["P_R",self.model_id,self.parameters_id])
        self.plot_precision_recall_n(model_name,pdf_report,plot_type='avg',output_type="save")#all
        self.plot_precision_recall_n(model_name,pdf_report,plot_type='normal',output_type="save")
        self.plot_precision_recall_n(model_name,pdf_report,time='day',plot_type='avg',output_type="save")#day
        self.plot_precision_recall_n(model_name,pdf_report,time='day',plot_type='normal',output_type="save")
        self.plot_precision_recall_n(model_name,pdf_report,time='night',plot_type='avg',output_type="save")#night
        self.plot_precision_recall_n(model_name,pdf_report,time='night',plot_type='normal',output_type="save")
            #print
        print("average night  p_1,r_1",self.p_avg_array['night'][0],self.r_avg_array['night'][0])
        print("average day p_8,r_8",self.p_avg_array['day'][7],self.r_avg_array['day'][7])
         
        #close
        pdf_report.close()
        return self.p_avg_array['day'][7],self.r_avg_array['day'][7]
