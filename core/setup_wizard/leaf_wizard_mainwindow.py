""" leaf_wizard_mainwindow.py

    Lab Equipment Adapter Framework (LEAF) Configurator Wizard GUI

        Class(es) to build the wizard window for setup

    REF:
        https://learn.microsoft.com/en-us/windows/win32/uxguide/win-wizards


"""

##################################
#
#            PACKAGES
#
###################################
import json

import tkinter as tk
from tkinter import ttk
from tkinter import messagebox
import tkinter.scrolledtext as st 

from PIL import Image, ImageTk




##############################################################
#                                                            #
#               LEAF Main window                             #
#                                                            #
#                                                            #
##############################################################
class MainWindow():
    """ MAIN WINDOW
    
    
    """

    def __init__(self, master):
        """
        
        
        """
        ##################################
        #
        #     VARIABLES
        #
        ###################################
        self.shared_variables = {
                                    'logo filepath' : 'assets/images/Transparent.png',
                                    'logo resize'   : True,
                                    'logo resize dims' :   (521,521), #(546,364),  # (2084,2084)
                                    #'animated logo filepath' : 'assets/images/Animated_Logo_White.mp4', #assets\images\Animated_Logo_White.mp4
                                    'font name'    : 'Corbel',
                                    'font name headers' : 'Overpass',
                                    'font name text body' : 'Montserrat',
                                    'font size'    : 15,
                                    'font size headers' : 16,#24,
                                    'font size text body' : 12,
                                    'color style'  : {
                                                        'main color'        : '#FF9538',       #'orange'    : '#FF9538',
                                                        'primary color'     : '#FF9538',       #'orange'    : '#FF9538',
                                                        'highlight color 1' : '#545454',   #'dark gray' : '#545454',
                                                        'highlight color 2' : '#44546A',   #'dark blue' : '#44546A',
                                                        'highlight color 3' : '#F6F6F6',   # opaque white 
                                                        'background color'  : '#F6F6F6',   # opaque white 
                                                        'secondary color 1' : '#CDCDCD',   # cool grey 2
                                                        'secondary color 2' : '#FCBD5C',   # 135C yellow-orange
                                                        'secondary color 3' : '#62B5E5',   # 2915C blue 
                                                    },  
                                    'screen width' : master.winfo_screenwidth(),
                                    'screen height': master.winfo_screenheight(),

                                    # FUNDING & LICENCE
                                    'funding logo filepath' : 'Images/Funding_logo_C.png',
                                    'funding text filepath' : 'assets/licenses_and_acknowledgements/acknowledgements.txt',
                                    'funding text' : '',

                                    'licence text filepath' : 'assets/licenses_and_acknowledgements/licence.txt',
                                    'licence text' : '',

                                    'user input variables' : dict(),
                                    
                                    }
        
        #--------------------------
        # Bottom button bar:
        buttonframe = tk.Frame(master, height = 64,
                            bg = self.shared_variables['color style']['highlight color 2'],

                            )
        buttonframe.grid_columnconfigure((0,1,2,3,4), weight =1, uniform= 'column')
        buttonframe.pack(#padx= 100, 
                      side = 'bottom', fill = 'x')
        
        #--------------------------
        # Sidebar:
        sideframe = tk.Frame(master, width = 164,
                            bg = self.shared_variables['color style']['main color'],

                            )
        sideframe.pack(#padx= 100, 
                      side = 'left', fill = 'y')
        

        
        #---------------------------
        # Body:
        main_body_frame = tk.Frame(master)
        main_body_frame.pack(fill = 'both', expand = 1)



        #---------------------------
        # Switch between frames:

        # First track current window being displayed
        self.index = 0

        # make a framelist to switch between
        self.framelist = [  # WINDOW 0:
                            LEAF_HomeWindow(main_body_frame, controller = self), 

                            # WINDOW 1: 
                            #CreateHomeWindow(main_body_frame, controller = self),
                            LEAF_EUAWindow(main_body_frame, controller = self), 


                            ]

        # make the first frame the default frame
        for idx in range(len(self.framelist)): 

            if idx == self.index:
                continue

            self.framelist[idx].forget()

        
        #----------------------
        # BUTTON(s): Next
        self.next_button = tk.Button(buttonframe, text = "Next", 
                                    bg = self.shared_variables['color style']['secondary color 1' ], 
                                    fg = self.shared_variables['color style']['highlight color 2'],
                                    font = ('Corbel', 12, 'bold'),
                                    command = lambda: self.change_to_another_window(window_index = self.index+1), 
                                    width = 6, height = 1)
        self.next_button.grid(row = 0, column = 3, padx = 2, pady = 15,sticky='E' ) #

        #----------------------
        # BUTTON(s): Cancel
        self.next_button = tk.Button(buttonframe, text = "Cancel", 
                                    bg = self.shared_variables['color style']['secondary color 1' ], 
                                    fg = self.shared_variables['color style']['highlight color 2'],
                                    font = ('Corbel', 12, 'bold'),
                                    command = lambda: self.cancel_(master), 
                                    width = 6, height = 1)
        self.next_button.grid(row = 0, column = 4, padx = 2, pady = 15, ) #sticky='E'

    #-----------------------------
    # MAIN FUNCTIONS
    def change_to_another_window(self, window_index):
        """ CHANGE WINDOW TO ANOTHER WINDOW

        
        """
        self.framelist[self.index].forget()

        # bring the initial frame up
        self.index = window_index
        self.framelist[self.index].tkraise()

        #repack frame
        self.framelist[self.index].pack(padx = 20, pady= 20)

    # def next_(self):
    #     """
        
    #     """
    #     pass

    def cancel_(self, window_):
        """ FUNCTION: cancel the current setup

            INPUT:
                window_ = the window to be destroyed
            
            OUTPUT:
                message box
                    if yes... destroy window
                    if no... return to window
        
        """
        #----------------
        # Create a warning message
        messagebox_cancel_setup = messagebox.askquestion(
                                                    title='Cancel setup',
                                                    message='Do you wish to stop the installation process?',
                                                    detail='All input will be lost',
                                                    icon = 'warning'
                                                    )


        if messagebox_cancel_setup == 'yes':
            
            window_.destroy()

        


##############################################################
#                                                            #
#               LEAF Home window                             #
#                                                            #
#                                                            #
##############################################################
class LEAF_HomeWindow(tk.Frame):

    def __init__(self, parent, controller):

        super().__init__(parent)

        self.controller=controller

        Home_frame = self

        #------------------------
        # MAKE FRAMES
        Home_frame_top    = tk.Frame(Home_frame, height = 100) #bg = 'white',
        Home_frame_middle = tk.Frame(Home_frame, height = 100)
        Home_frame_bottom = tk.Frame(Home_frame,height = 100)

        #-------------------------
        # pack frames
        Home_frame_top.pack(side    = 'top', fill = 'both')
        Home_frame_middle.pack(fill = 'both', expand = True)
        Home_frame_bottom.pack(side = 'bottom', fill = 'both')

        #--------------------------
        # Top frame:
        Title_label = tk.Label(Home_frame_top, text = 'Welcome to the Lab Equipment Adapter Framework (LEAF)\n Setup Wizard',
                               font = (self.controller.shared_variables['font name'], self.controller.shared_variables['font size headers'], 'bold', ),
                               fg = self.controller.shared_variables['color style']['highlight color 2'],   
                               justify='left',
                               ).pack(padx = 10, pady= 20)        
        
        #--------------------------
        # Middle frame:
        Description = tk.Label(Home_frame_middle, text = 'This installation wizard will guide you through the process of installing LEAF     \nonto your computer. Before you begin make sure that you have the details \nof the device that you intend to setup (manufacturer, API token, etc.) to hand.\n\nTo start the installation, click the Next button.',
                               font = (self.controller.shared_variables['font name'], self.controller.shared_variables['font size text body'], 
                                       #'bold', 
                                       ),
                               fg = self.controller.shared_variables['color style']['highlight color 1'],   
                               justify='left'
                               ).pack(padx = 10, pady= 20) 
        
        #---------------------------
        # PACK FRAME
        self.pack(padx = 20, pady =20)      




##############################################################
#                                                            #
#               LEAF End user agreement                      #
#                                                            #
#                                                            #
##############################################################
class LEAF_EUAWindow(tk.Frame):

    def __init__(self, parent, controller):

        super().__init__(parent)

        self.controller=controller

        Home_frame = self

        #------------------------
        # MAKE FRAMES
        Home_frame_top    = tk.Frame(Home_frame, height = 100) #bg = 'white',
        Home_frame_middle = tk.Frame(Home_frame, height = 100)
        Home_frame_bottom = tk.Frame(Home_frame,height = 100)

        #-------------------------
        # pack frames
        Home_frame_top.pack(side    = 'top', fill = 'both')
        Home_frame_middle.pack(fill = 'both', expand = True)
        Home_frame_bottom.pack(side = 'bottom', fill = 'both')

        #--------------------------
        # Top frame:
        Title_label = tk.Label(Home_frame_top, text = 'End user agreement',
                               font = (self.controller.shared_variables['font name'], self.controller.shared_variables['font size headers'], 'bold', ),
                               fg = self.controller.shared_variables['color style']['highlight color 2'],   
                               justify='left',
                               ).pack(padx = 10, pady= 20)        
        
        #--------------------------
        # Middle frame:
        # Description = tk.Label(Home_frame_middle, text = 'This installation wizard will guide you through the process of installing LEAF     \nonto your computer. Before you begin make sure that you have the details \nof the device that you intend to setup (manufacturer, API token, etc.) to hand.\n\nTo start the installation, click the Next button.',
        #                        font = (self.controller.shared_variables['font name'], self.controller.shared_variables['font size text body'], 
        #                                #'bold', 
        #                                ),
        #                        fg = self.controller.shared_variables['color style']['highlight color 1'],   
        #                        justify='left'
        #                        ).pack(padx = 10, pady= 20) 
        
        #---------------------------
        # PACK FRAME
        self.pack(padx = 20, pady =20)     