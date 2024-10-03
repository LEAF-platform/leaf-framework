""" leaf_wizard.py
    
    Lab Equipment Adapter Framework (LEAF) Configurator Wizard Wrapper

        Wrapper to call the wizard

    REF:
        https://learn.microsoft.com/en-us/windows/win32/uxguide/win-wizards

"""

##################################
#
#            PACKAGES
#
###################################
#import os
#import json 
#import pandas as pd

#import ttkbootstrap as tb
import tkinter as tk
from tkinter import ttk
from tkinter import messagebox
import tkinter.scrolledtext as st 

from leaf_wizard_mainwindow import MainWindow


##################################
#
#     VARIABLES
#
###################################
wizard_app_settings = {
                            "version"    : '0.0.1',
                            "app_height" : 500,
                            "app_width"  : 800,
                            "app_title"  : "Lab Equipment Adapter Framework (LEAF) Setup Tool",
                            "end user agreement filepath":  "End User Agreement.txt",
                        }



##################################
#
#     FUNCTIONS
#
###################################
def wizard_app(settings_dict): 
    """
    
    
    """
    #--------------------------------
    # CREATE ROOT
    root = tk.Tk()
    root.title(settings_dict["app_title"])

    
    #-------------------------------
    # CREATE GEOMETRY
    #root.state("zoomed")
    #root.geometry("1536x1024")

    app_width  = settings_dict["app_width"]
    app_height = settings_dict["app_height"]
    screen_width  = root.winfo_screenwidth()
    screen_height = root.winfo_screenheight()

    screen_x = (screen_width/2)  - (app_width/2)
    screen_y = (screen_height/2) - (app_height/2)-25

    root.geometry("%dx%d+%d+%d" % (app_width, app_height, screen_x, screen_y))


    #-------------------------------
    # CREATE WINDOW
    window = MainWindow(root)
    root.mainloop()



##################################
#
#      MAIN
#
###################################
if __name__ == '__main__':
    

    wizard_app(wizard_app_settings)