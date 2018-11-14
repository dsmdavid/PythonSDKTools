import AlteryxPythonSDK as Sdk
import matplotlib.pyplot as plt
import xml.etree.ElementTree as Et
import pandas as pd
import random
import seaborn as sns
from itertools import cycle

class AyxPlugin:
    """
    Implements the plugin interface methods, to be utilized by the Alteryx engine to communicate with a plugin.
    Prefixed with "pi", the Alteryx engine will expect the below five interface methods to be defined.
    """

    def __init__(self, n_tool_id: int, alteryx_engine: object, output_anchor_mgr: object):
        """
        Constructor is called whenever the Alteryx engine wants to instantiate an instance of this plugin.
        :param n_tool_id: The assigned unique identification for a tool instance.
        :param alteryx_engine: Provides an interface into the Alteryx engine.
        :param output_anchor_mgr: A helper that wraps the outgoing connections for a plugin.
        """

        # Default properties
        self.n_tool_id = n_tool_id
        self.alteryx_engine = alteryx_engine
        self.output_anchor_mgr = output_anchor_mgr

        # Custom properties
        # self.is_valid = False
        self.single_input = None
        self.despine= True #Default 
        self.trim = True
        self.remove_legend = False
        self.plot_violin = False
        self.plot_boxplot = False
        self.key_var = None

    def pi_init(self, str_xml: str):
        """
        Handles building out the sort info from the user configuration.
        Called when the Alteryx engine is ready to provide the tool configuration from the GUI.
        :param str_xml: The raw XML from the GUI.
        """
        


        if Et.fromstring(str_xml).find('FieldSelectMulti').text is not None:
            self.field_selection = Et.fromstring(str_xml).find('FieldSelectMulti').text.split(",")
            self.is_valid = True
        else:
            self.alteryx_engine.output_message(self.n_tool_id, Sdk.EngineMessageType.error,
                                               'Please select fields for melting the data')
        info_msg = 'Field selection: '+str(self.field_selection)
        #self.alteryx_engine.output_message(self.n_tool_id, Sdk.EngineMessageType.info, self.xmsg('Field selection: '+str(self.field_selection)))

        if Et.fromstring(str_xml).find('DataField').text is not None:
            self.key_var = Et.fromstring(str_xml).find('DataField').text.split(",")[0]
            self.is_valid = True
        else:
            self.alteryx_engine.output_message(self.n_tool_id, Sdk.EngineMessageType.info,
                                               'No key selected')
        
        info_msg += '; Field for key: '+str(self.key_var)
        #self.alteryx_engine.output_message(self.n_tool_id, Sdk.EngineMessageType.info, self.xmsg('Field for key: '+str(self.key_var)))

        if Et.fromstring(str_xml).find('ColorField').text is not None:
            self.color_var = Et.fromstring(str_xml).find('ColorField').text.split(",")[0]
            info_msg += '; Field for color: '+str(self.color_var)
            #self.alteryx_engine.output_message(self.n_tool_id, Sdk.EngineMessageType.info, self.xmsg('Field for color: '+str(self.color_var)))
        else:
            self.color_var = ''
            info_msg += '; Field for color not selected, color will be randomly assigned'
            #self.alteryx_engine.output_message(self.n_tool_id, Sdk.EngineMessageType.info, self.xmsg('Field for color not selected, color will be randomly assigned'))
            
        if self.color_var != '' and self.key_var is None:
            self.alteryx_engine.output_message(self.n_tool_id, Sdk.EngineMessageType.error,
                                               'A color column cannot be passed if no key was selected')
        
        
        self.alteryx_engine.output_message(self.n_tool_id, Sdk.EngineMessageType.info, self.xmsg(info_msg))
        
        self.alteryx_engine.output_message(self.n_tool_id, Sdk.EngineMessageType.info,
                                               'N1')
        
        
        if Et.fromstring(str_xml).find('CheckBoxDespine').text is not None:
            if Et.fromstring(str_xml).find('CheckBoxDespine').text =='True': ##Default is True
                self.despine =  False
            if self.despine:
                msg_desp = 'Despine ON'
            else:
                msg_desp = 'Despine Disabled'
        self.alteryx_engine.output_message(self.n_tool_id, Sdk.EngineMessageType.info,
                                               'N2')
        if Et.fromstring(str_xml).find('CheckBoxLegend').text is not None:
            if Et.fromstring(str_xml).find('CheckBoxLegend').text =='True': ##Default is False
                self.remove_legend =  True
            if self.remove_legend:
                msg_desp += ', Legend Disabled'
            else:
                msg_desp += ', Legend ON'         
        self.alteryx_engine.output_message(self.n_tool_id, Sdk.EngineMessageType.info,
                                               'N3')
        if Et.fromstring(str_xml).find('CheckBoxTrim').text is not None:
            if Et.fromstring(str_xml).find('CheckBoxTrim').text =='True': ##Default is True
                self.trim =  False
            if self.trim:
                msg_desp += ', Trimming ON'
            else:
                msg_desp += ', Trimming Disabled'
        self.alteryx_engine.output_message(self.n_tool_id, Sdk.EngineMessageType.info,
                                               'N4')
        if Et.fromstring(str_xml).find('DropDownOverlay1').text is not None:
            selection = Et.fromstring(str_xml).find('DropDownOverlay1').text
            if selection == 'nothing':
                pass
            elif selection =='violin':
                self.plot_violin = True
            elif selection == 'boxplot':
                self.plot_boxplot = True
            msg_desp += '; ' + selection + ' overlay'
        self.alteryx_engine.output_message(self.n_tool_id, Sdk.EngineMessageType.info,
                                               'N5')
        self.alteryx_engine.output_message(self.n_tool_id, Sdk.EngineMessageType.info, self.xmsg(msg_desp))
        
        
        
        
        # Getting the output anchor from the XML file.
        self.output_anchor = self.output_anchor_mgr.get_output_anchor('Output')

    def pi_add_incoming_connection(self, str_type: str, str_name: str) -> object:
        """
        The IncomingInterface objects are instantiated here, one object per incoming connection, also pre_sort.
        Called when the Alteryx engine is attempting to add an incoming data connection.
        :param str_type: The name of the input connection anchor, defined in the Config.xml file.
        :param str_name: The name of the wire, defined by the workflow author.
        :return: The IncomingInterface object(s).
        """

        self.single_input = IncomingInterface(self)
        return self.single_input

    def pi_add_outgoing_connection(self, str_name: str) -> bool:
        """
        Called when the Alteryx engine is attempting to add an outgoing data connection.
        :param str_name: The name of the output connection anchor, defined in the Config.xml file.
        :return: True signifies that the connection is accepted.
        """

        return True

    def pi_push_all_records(self, n_record_limit: int) -> bool:
        """
        Called when a tool has no incoming data connection.
        :param n_record_limit: Set it to <0 for no limit, 0 for no records, and >0 to specify the number of records.
        :return: True for success, False for failure.
        """

        self.alteryx_engine.output_message(self.n_tool_id, Sdk.EngineMessageType.error, self.xmsg('Missing Incoming Connection'))
        return False

    def pi_close(self, b_has_errors: bool):
        """
        Called after all records have been processed..
        :param b_has_errors: Set to true to not do the final processing.
        """

        self.output_anchor.assert_close() # Checks whether connections were properly closed.

        #pass
    def build_record_info_out(self,obj):
        """
        A non-interface helper for pi_push_all_records() responsible for creating the outgoing record layout.
        :param file_reader: The name for csv file reader.
        :return: The outgoing record layout, otherwise nothing.
        """
        
        record_info_out = Sdk.RecordInfo(self.alteryx_engine)  # A fresh record info object for outgoing records.

        
        #if obj.loc[0,'data']=='image': #png read

        try:
            for i in obj.columns:
                if i == 'data' or i =='dataframe':
                    record_info_out.add_field(str(i), Sdk.FieldType.v_wstring, size=50)
                elif i =='swarmplot':
                    record_info_out.add_field(str(i), Sdk.FieldType.blob)
                else:
                    self.display_error_msg("failed " + str(i))

                    

        except:
            record_info_out.add_field(str(i), Sdk.FieldType.int16)
            self.display_error_msg("try errored " + str(i))


        return record_info_out
    
    def xmsg(self, msg_string: str):
        """
        A non-interface, non-operational placeholder for the eventual localization of predefined user-facing strings.
        :param msg_string: The user-facing string.
        :return: msg_string
        """

        return msg_string
    
    def display_error_msg(self, msg_string: str):
        """
        A non-interface method, that is responsible for displaying the relevant error message in Designer.
        :param msg_string: The custom error message.
        """

        self.is_initialized = False
        self.alteryx_engine.output_message(self.n_tool_id, Sdk.EngineMessageType.error, self.xmsg(msg_string))

class IncomingInterface:
    """
    This class is returned by pi_add_incoming_connection, and it implements the incoming interface methods, to be\
    utilized by the Alteryx engine to communicate with a plugin when processing an incoming connection.
    Prefixed with "ii", the Alteryx engine will expect the below four interface methods to be defined.
    """

    def __init__(self, parent: object):
        """
        Constructor for IncomingInterface.
        :param parent: AyxPlugin
        """

        # Default properties
        self.parent = parent

        # Custom properties
        self.record_info_in = None
        self.selected_columns = self.parent.field_selection
        self.key_var=self.parent.key_var
        self.color_var= self.parent.color_var
        self.field_lists = []
        self.in_record_list = []
        self.counter = 0
        self.despine= self.parent.despine #Default 
        self.trim = self.parent.trim
        self.remove_legend = self.parent.remove_legend
        self.plot_violin = self.parent.plot_violin
        self.plot_boxplot = self.parent.plot_boxplot
        if self.plot_violin or self.plot_boxplot:
            self.alpha= 0.3
        else:
            self.alpha = 1

    def ii_init(self, record_info_in: object) -> bool:
        """
        Called to report changes of the incoming connection's record metadata to the Alteryx engine.
        :param record_info_in: A RecordInfo object for the incoming connection's fields.
        :return: True for success, otherwise False.
        """
        self.record_info_in = record_info_in

        # returns a new empty RecordCreator object that is identical to record_info_in.
        record_info_out = record_info_in.clone()

        # Storing the field names to use when creating the dataframe.
        for field in range(record_info_in.num_fields):
            self.field_lists.append([record_info_in[field].name])

        return True

    def ii_push_record(self, in_record: object) -> bool:
        """
        Called when an input record is being sent to the plugin.
        :param in_record: The data for the incoming record.
        :return: False if method calling limit (record_cnt) is hit.
        """

        # Storing the string data of in_record
        for field in range(self.record_info_in.num_fields):
            in_value = self.record_info_in[field].get_as_string(in_record)
            self.field_lists[field].append(in_value) if in_value is not None else self.field_lists[field].append('')

        return True

    def ii_update_progress(self, d_percent: float):
        """
        Called by the upstream tool to report what percentage of records have been pushed.
        :param d_percent: Value between 0.0 and 1.0.
        """

        self.parent.alteryx_engine.output_tool_progress(self.parent.n_tool_id, d_percent)  # Inform the Alteryx engine of the tool's progress

    def ii_close(self):
        """
        Called when the incoming connection has finished passing all of its records.
        """
        #Create Dataframe based on the stored values:
        #debug with open(r'C:\Users\DavidSM\Desktop\tmp\output_field_list.txt', "w") as fieldFile:
        #debug   fieldFile.write(str(self.field_lists))
        #create the dataframe, reshape
        self.input_dataframe = pd.DataFrame.from_records(self.field_lists).T
        self.input_dataframe.columns = self.input_dataframe.iloc[0]
        self.input_dataframe = self.input_dataframe[self.input_dataframe.index>0]
  

        #retrieve graph data_frame


        self.df = self.graph_output(self.selected_columns, self.key_var, self.color_var)
        
        record_info_out = self.parent.build_record_info_out(self.df)

        
        
        # Lets the downstream tools know what the outgoing record metadata will look like, based on record_info_out.
        self.parent.output_anchor.init(record_info_out)

        # Creating a new, empty record creator based on record_info_out's record layout.
        record_creator = record_info_out.construct_record_creator()   
        #for field in range(record_info_out.num_fields):
        #    self.display_error_msg(record_info_out[field].name)
        
        

        for row in self.df.index:
            t=0
            for column in self.df.columns:
                if column == 'data':
                    record_info_out[t].set_from_string(record_creator,str(self.df.loc[row,column]))
                else:
                    record_info_out[t].set_from_blob(record_creator,(self.df.loc[row,column]))
                t+=1
        
            out_record = record_creator.finalize_record()
            self.parent.output_anchor.push_record(out_record, False)  # False: completed connections will automatically close.
            record_creator.reset()  # Resets the variable length data to 0 bytes (default) to prevent unexpected results.
            
        # Make sure that the output anchor is closed.
        self.parent.output_anchor.close()
        
 
    def graph_output(self, selected_columns, key_var, color_var):
        """Will generate the graph"""
        
        
        #Load df
        selected_columns = selected_columns
        key_var = key_var
        color_var = color_var
        keep_columns = selected_columns[:]
        if key_var is not None:
            keep_columns.append(key_var)
        if color_var != '':
            keep_columns.append(color_var)


        df = self.input_dataframe[keep_columns]
       #debug  df.to_csv(r'C:\Users\DavidSM\Desktop\tmp\before_melt_df.csv')
        
        ###Get the colors
        dict_colors = {}
        
        #default colors available if not provided
        l_colors = ['aliceblue', 'antiquewhite', 'aqua', 'aquamarine', 'azure', 'beige', 'bisque', 'black','blanchedalmond', 'blue', 'blueviolet', 'brown', 'burlywood', 'cadetblue', 'chartreuse', 'chocolate', 'coral', 'cornflowerblue', 'cornsilk', 'crimson', 'cyan', 'darkblue', 'darkcyan', 'darkgoldenrod', 'darkgray', 'darkgreen', 'darkgrey', 'darkkhaki', 'darkmagenta', 'darkolivegreen', 'darkorange', 'darkorchid', 'darkred', 'darksalmon', 'darkseagreen', 'darkslateblue', 'darkslategray', 'darkslategrey', 'darkturquoise', 'darkviolet', 'deeppink', 'deepskyblue', 'dimgray', 'dimgrey', 'dodgerblue', 'firebrick', 'floralwhite', 'forestgreen', 'fuchsia', 'gainsboro', 'ghostwhite', 'gold', 'goldenrod', 'gray', 'green', 'greenyellow', 'grey', 'honeydew', 'hotpink', 'indianred', 'indigo', 'ivory', 'khaki', 'lavender', 'lavenderblush', 'lawngreen', 'lemonchiffon', 'lightblue', 'lightcoral', 'lightcyan', 'lightgoldenrodyellow', 'lightgray', 'lightgreen', 'lightgrey', 'lightpink', 'lightsalmon', 'lightseagreen', 'lightskyblue', 'lightslategray', 'lightslategrey', 'lightsteelblue', 'lightyellow', 'lime', 'limegreen', 'linen', 'magenta', 'maroon', 'mediumaquamarine', 'mediumblue', 'mediumorchid', 'mediumpurple', 'mediumseagreen', 'mediumslateblue', 'mediumspringgreen', 'mediumturquoise', 'mediumvioletred', 'midnightblue', 'mintcream', 'mistyrose', 'moccasin', 'navajowhite', 'navy', 'oldlace', 'olive', 'olivedrab', 'orange', 'orangered', 'orchid', 'palegoldenrod', 'palegreen', 'paleturquoise', 'palevioletred', 'papayawhip', 'peachpuff', 'peru', 'pink', 'plum', 'powderblue', 'purple', 'rebeccapurple', 'red', 'rosybrown', 'royalblue', 'saddlebrown', 'salmon', 'sandybrown', 'seagreen', 'seashell', 'sienna', 'silver', 'skyblue', 'slateblue', 'slategray', 'slategrey', 'snow', 'springgreen', 'steelblue', 'tan', 'teal', 'thistle', 'tomato', 'turquoise', 'violet', 'wheat', 'white', 'whitesmoke', 'yellow', 'yellowgreen']
        random.shuffle(l_colors)
        c_colors = cycle(l_colors)
        #Colors are provided
        if color_var != '':
            if key_var is not None: #Colors cannot be provided without a key_var
                #Get colors from column for dictionary
                gc = df.pivot(columns=key_var, values=color_var)
                
                for un in gc.columns.unique():
                    dict_colors[un] = gc[gc[un].notnull()].iloc[0][un]

                #Drop color from df
                df.drop(color_var, axis=1, inplace=True)
        #Colors are not provided
            ## If no color is given, then take the default named colors, randomize and cycle --don't think anyone
            ## is going to use more than 148 colors, but...
        else:
            if key_var is not None: #If key_var exists, assign color to unique values in the key_var
                for un in df[key_var].unique():
                    dict_colors[un] = next(c_colors)
            else:                   #If key_var doesn't exist, assign color based on measurement
                temp = pd.melt(df, var_name="measurement")
                for un in temp["measurement"].unique():
                    dict_colors[un] = next(c_colors)
                


        #debug df.to_csv(r'C:\Users\DavidSM\Desktop\tmp\melted_df.csv')
    
        df = pd.melt(df,key_var, var_name="measurement")


        
        df.value = df.value.apply(lambda x: float(x))
        

        #debug df.to_csv(r'C:\Users\DavidSM\Desktop\tmp\melted_df.csv')

        fig, ax = plt.subplots()

        if key_var is not None:
            sns.swarmplot(x=df["measurement"], y=df.value, hue=df[key_var], palette=dict_colors, ax=ax, alpha=self.alpha)
        else:
            sns.swarmplot(x=df["measurement"], y=df.value, palette=dict_colors, ax=ax, alpha=self.alpha)
        
         #   ViolinPlots? Boxplots?
        if self.plot_violin:
            if key_var is not None:
                sns.violinplot(x=df["measurement"], y=df.value, hue=df[key_var], palette=dict_colors, ax=ax)
            else:
                sns.violinplot(x=df["measurement"], y=df.value, palette=dict_colors, ax=ax)
        if self.plot_boxplot:
            if key_var is not None:
                sns.boxplot(x=df["measurement"], y=df.value, hue=df[key_var], palette=dict_colors, ax=ax)
            else:
                sns.boxplot(x=df["measurement"], y=df.value, palette=dict_colors, ax=ax)
                
        #Apply settings despine, trim and remove legend:
        if self.despine:
            sns.despine(ax=ax, trim=self.trim)
        if self.remove_legend:
            ax.legend_.remove() 
            
        fig.savefig('tempfig.png',format='png')


        with open('tempfig.png', "rb") as imageFile:
            encoded_str =imageFile.read()
        test = {'swarmplot':encoded_str, 'data':'Add Image Tool on "swarmplot"'}
        df = pd.DataFrame.from_dict(test, orient='index').T


            
        ##Surely there's no need to save this to disk and then read it as binary,
        ##but don't know how.
        #r'C:\Users\DavidSM\Desktop\tmp\tempfig.png'



        
        return df

