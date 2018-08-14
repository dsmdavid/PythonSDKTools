"""
AyxPlugin (required) has-a IncomingInterface (optional).
Although defining IncomingInterface is optional, the interface methods are needed if an upstream tool exists.
"""

import AlteryxPythonSDK as Sdk
import xml.etree.ElementTree as Et
import scipy.special
import pandas as pd

class AyxPlugin:
    """
    Implements the plugin interface methods, to be utilized by the Alteryx engine to communicate with this plugin.
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
        self.is_initialized = True
        self.output_anchor = None
        self.n_rows = None


    def pi_init(self, str_xml: str):
        """
        This tool is only taking Number of rows as user input.
        Called when the Alteryx engine is ready to provide the tool configuration from the GUI.
        :param str_xml: The raw XML from the GUI.
        """
        #Initialize Output
        self.output_anchor = self.output_anchor_mgr.get_output_anchor('Output')
        # Getting the user-entered selections from the GUI.
        temp_n_rows = Et.fromstring(str_xml).find('NRows').text
        try:
            self.n_rows = int(temp_n_rows)
        except ValueError:
            self.n_rows = 5
            self.display_error_msg('Number of rows is not an integer! Defaulting to  5 rows.','warning')
        except TypeError:
            self.n_rows = 5
            self.display_error_msg('Invalid number of rows! Defaulting to  5 rows.','warning')

        # Limit the number of rows to 10000
        if self.n_rows > 100:
            self.n_rows = 100
            self.display_error_msg('Maximum number of rows reached, capped at 100','warning')
            
            
        pass

    def pi_add_incoming_connection(self, str_type: str, str_name: str) -> object:
        """
        The IncomingInterface objects are instantiated here, one object per incoming connection, however since
        this tool does not accept an incoming connection, instantiation is not needed and "ii" methods won't be called.
        Called when the Alteryx engine is attempting to add an incoming data connection.
        :param str_type: The name of the input connection anchor, defined in the Config.xml file.
        :param str_name: The name of the wire, defined by the workflow author.
        :return: self.
        """

        return self

    def pi_add_outgoing_connection(self, str_name: str) -> bool:
        """
        Called when the Alteryx engine is attempting to add an outgoing data connection.
        :param str_name: The name of the output connection anchor, defined in the Config.xml file.
        :return: True signifies that the connection is accepted.
        """

        return True

    def pi_push_all_records(self, n_record_limit: int) -> bool:
        """
        Handles pushing records out to downstream tool(s).
        Called when a tool has no incoming data connection.
        :param n_record_limit: Set it to <0 for no limit, 0 for no records, and >0 to specify the number of records.
        :return: False if there are issues with the input data or if the workflow isn't being ran, otherwise True.
        """

        self.dataframe = self.Pascal(self.n_rows)
        record_info_out = self.build_record_info_out()  # Building out the outgoing record layout.
        self.output_anchor.init(record_info_out)  # Lets the downstream tools know of the outgoing record metadata.
        record_creator = record_info_out.construct_record_creator()  # Creating a new record_creator for the new data.
        
        for row in self.dataframe.index:
            t=0
            for column in self.dataframe.columns:
                record_info_out[t].set_from_string(record_creator,str(self.dataframe.loc[row,column]))
                t+=1
        
            out_record = record_creator.finalize_record()
            self.output_anchor.push_record(out_record, False)  # False: completed connections will automatically close.
            record_creator.reset()  # Resets the variable length data to 0 bytes (default) to prevent unexpected results.

        self.alteryx_engine.output_message(self.n_tool_id, Sdk.EngineMessageType.info, self.xmsg(
        str(self.n_rows)+' records were processed.'))
        self.output_anchor.close()  # Close outgoing connections.
        return True

    def pi_close(self, b_has_errors: bool):
        """
        Called after all records have been processed.
        :param b_has_errors: Set to true to not do the final processing.
        """

        self.output_anchor.assert_close()  # Checks whether connections were properly closed.


    def build_record_info_out(self):
        """
        A non-interface helper for pi_push_all_records() responsible for creating the outgoing record layout.
        :param file_reader: The name for csv file reader.
        :return: The outgoing record layout, otherwise nothing.
        """

        record_info_out = Sdk.RecordInfo(self.alteryx_engine)  # A fresh record info object for outgoing records.
        #We are returning a dataframe with M rows and M*2 columns.
        #self.n_columns = self.n_rows*2
        for i in self.dataframe.columns:
            record_info_out.add_field(str(i), Sdk.FieldType.string, 254)
        return record_info_out

    def display_error_msg(self, msg_string: str, msg_type: str):
        """
        A non-interface method, that is responsible for displaying the relevant error message in Designer.
        :param msg_string: The custom error message.
        """

        self.is_initialized = False
        if msg_type == 'info':
            self.alteryx_engine.output_message(self.n_tool_id, Sdk.EngineMessageType.info, self.xmsg(msg_string))
        elif msg_type == 'warning':
            self.alteryx_engine.output_message(self.n_tool_id, Sdk.EngineMessageType.warning, self.xmsg(msg_string))
        elif msg_type == 'error':
            self.alteryx_engine.output_message(self.n_tool_id, Sdk.EngineMessageType.error, self.xmsg(msg_string))

    def xmsg(self, msg_string: str):
        """
        A non-interface, non-operational placeholder for the eventual localization of predefined user-facing strings.
        :param msg_string: The user-facing string.
        :return: msg_string
        """

        return msg_string
    
    def Pascal(self, value):
        '''Returns the Pascal Triangle up to the Row defined in the value'''
        # Instantiate the df will need double the number of 
        # columns as they don't stack
        
        df = pd.DataFrame(0, index= range(value+1), columns = range((value+1)*2-1)) 
        for index in df.index:
            diff = len(df) - (index+1) #calculate the step to add to the stair
            for column in df.columns:
                try:
                    df.iloc[index,(column*2 + diff)] = int(scipy.special.binom(index, column))
                except: pass
        df.replace(to_replace=0,value='',inplace=True)# remove zeros
        return df


class IncomingInterface:
    """
    This optional class is returned by pi_add_incoming_connection, and it implements the incoming interface methods, to
    be utilized by the Alteryx engine to communicate with a plugin when processing an incoming connection.
    Prefixed with "ii", the Alteryx engine will expect the below four interface methods to be defined.
    """

    def __init__(self, parent: object):
        """
        Constructor for IncomingInterface.
        :param parent: AyxPlugin
        """

        pass

    def ii_init(self, record_info_in: object) -> bool:
        """
        Called to report changes of the incoming connection's record metadata to the Alteryx engine.
        :param record_info_in: A RecordInfo object for the incoming connection's fields.
        """

        pass

    def ii_push_record(self, in_record: object) -> bool:
        """
        Called when an input record is being sent to the plugin.
        :param in_record: The data for the incoming record.
        """

        pass

    def ii_update_progress(self, d_percent: float):
        """
        Called by the upstream tool to report what percentage of records have been pushed.
        :param d_percent: Value between 0.0 and 1.0.
        """

        pass

    def ii_close(self):
        """
        Called when the incoming connection has finished passing all of its records.
        """

        pass
