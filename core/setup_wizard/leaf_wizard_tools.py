""" leaf_wizard_tools.py

    Lab Equipment Adapter Framework (LEAF) Configurator Wizard Tools

        Functions and tools to aid in setup


"""


def read_in_text(text_to_read):
    """ FUNCTION: Read in text documents such as funding or licence text
            
        INPUT:
            text_to_read = licence or funding text to read in

        OUTPUT:
            tmp_text = read in text
            
        EXAMPLE:
            self.shared_variables['funding text'] = read_in_text(self.shared_variables['funding text filepath'])

    """
    #------------------------------------------
    # read in funding text  
    with open(text_to_read, 'r') as file:
                
        tmp_text = file.read().replace("\\n", "\n")
            
    return tmp_text
                