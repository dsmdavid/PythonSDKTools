import AlteryxPythonSDK as Sdk
import xml.etree.ElementTree as Et
import pandas as pd

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
        self.max_width = None
        self.number_rows = None
        self.starting_pos = None
        #self.input: IncomingInterface = None
        self.DataFrame: Sdk.OutputAnchor = None
        self.LastRow: Sdk.OutputAnchor = None

    def pi_init(self, str_xml: str):
        """
        Handles configuration based on the GUI.
        Called when the Alteryx engine is ready to provide the tool configuration from the GUI.
        :param str_xml: The raw XML from the GUI.
        """

        # Getting the starting values from the Gui.html
        self.max_width = int(Et.fromstring(str_xml).find('NumberSlots').text) if 'NumberSlots' in str_xml else None
        self.number_rows = int(Et.fromstring(str_xml).find('NumberRows').text) if 'NumberRows' in str_xml else None
        self.starting_pos = int(Et.fromstring(str_xml).find('StartingPos').text) if 'StartingPos' in str_xml else None

        # Valid checks.
        if self.starting_pos is None:
            self.display_error_msg('Starting Position cannot be empty.')
        elif self.starting_pos > self.max_width:
            self.display_error_msg('Starting Position cannot be greater than the number of slots.')

        # Getting the output anchor from Config.xml by the output connection name
        self.LastRow = self.output_anchor_mgr.get_output_anchor('LastRow')
        self.DataFrame = self.output_anchor_mgr.get_output_anchor('DataFrame')
        
        self.df, self.last_row = self.plinko_stat()


    def pi_add_incoming_connection(self, str_type: str, str_name: str) -> object:
        """
        The IncomingInterface objects are instantiated here, one object per incoming connection.
        Called when the Alteryx engine is attempting to add an incoming data connection.
        :param str_type: The name of the input connection anchor, defined in the Config.xml file.
        :param str_name: The name of the wire, defined by the workflow author.
        :return: The IncomingInterface object(s).
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
        Handles generating a new field for no incoming connections.
        Called when a tool has no incoming data connection.
        :return: False if there's an error with the field name, otherwise True.
        """
        
        ##Exporting the dataframe:
        # Save a reference to the RecordInfo passed into this function in the global namespace, so we can access it later.
        record_info_out = self.build_record_info_out(self.df)  # Building out the outgoing record layout.

        # Lets the downstream tools know what the outgoing record metadata will look like, based on record_info_out.
        self.DataFrame.init(record_info_out)

        # Creating a new, empty record creator based on record_info_out's record layout.
        record_creator = record_info_out.construct_record_creator()    
        for row in self.df.index:
            t=0
            for column in self.df.columns:
                record_info_out[t].set_from_string(record_creator,str(self.df.loc[row,column]))
                t+=1
        
            out_record = record_creator.finalize_record()
            self.DataFrame.push_record(out_record, False)  # False: completed connections will automatically close.
            record_creator.reset()  # Resets the variable length data to 0 bytes (default) to prevent unexpected results.
            
        # Make sure that the output anchor is closed.
        self.DataFrame.close()

        
        ##Exporting the lastrow:
        # Save a reference to the RecordInfo passed into this function in the global namespace, so we can access it later.
        record_info_out = self.build_record_info_out(self.last_row)  # Building out the outgoing record layout.

        # Lets the downstream tools know what the outgoing record metadata will look like, based on record_info_out.
        self.LastRow.init(record_info_out)

        # Creating a new, empty record creator based on record_info_out's record layout.
        record_creator = record_info_out.construct_record_creator()    
        for row in self.last_row.index:
            record_info_out[0].set_from_string(record_creator,str(row))
            record_info_out[1].set_from_string(record_creator,str(self.last_row[row]))

            
        
            out_record = record_creator.finalize_record()
            self.LastRow.push_record(out_record, False)  # False: completed connections will automatically close.
            #self.output_anchor.push_record(out_record, False)  # False: completed connections will automatically close.
            record_creator.reset()  # Resets the variable length data to 0 bytes (default) to prevent unexpected results.
            
        # Make sure that the output anchor is closed.
        self.LastRow.close()
        #self.output_anchor.close()
        
        
        return True

    def build_record_info_out(self,obj):
        """
        A non-interface helper for pi_push_all_records() responsible for creating the outgoing record layout.
        :param file_reader: The name for csv file reader.
        :return: The outgoing record layout, otherwise nothing.
        """

        record_info_out = Sdk.RecordInfo(self.alteryx_engine)  # A fresh record info object for outgoing records.
        #We are returning a dataframe with M rows and M*2 columns.
        try:
            for i in obj.columns:
                record_info_out.add_field(str(i), Sdk.FieldType.float)
        except:
            record_info_out.add_field(str('Position'), Sdk.FieldType.int16)
            record_info_out.add_field(str('Value'), Sdk.FieldType.float)

        return record_info_out    
    
    
    def pi_close(self, b_has_errors: bool):
        """
        Called after all records have been processed.
        :param b_has_errors: Set to true to not do the final processing.
        """

        # Checks whether connections were properly closed.
        self.DataFrame.assert_close()

    def display_error_msg(self, msg_string: str):
        """
        A non-interface method, that is responsible for displaying the relevant error message in Designer.
        :param msg_string: The custom error message.
        """

        self.is_initialized = False
        self.alteryx_engine.output_message(self.n_tool_id, Sdk.EngineMessageType.error, self.xmsg(msg_string))

    def xmsg(self, msg_string: str):
        """
        A non-interface, non-operational placeholder for the eventual localization of predefined user-facing strings.
        :param msg_string: The user-facing string.
        :return: msg_string
        """

        return msg_string
    
    
    def plinko_stat(self):
        "Will generate the DF with all possibilities based on starting and ending positions"
        max_width = self.max_width
        number_rows = self.number_rows
        starting_position = self.starting_pos
        df = pd.DataFrame([range(1,max_width*2)]*number_rows)
        for x in df.columns:
            for y in df.index:
                df.iloc[y,x] =0
        count = 1
        for y in df.columns:
            if count ==starting_position:
                df.iloc[0,y] = 1
            count+=1

        for x in df.index:
            if x ==0:
                pass
            else:
                for y in df.columns:
                    val = 0
                    try:
                        if y-1 >=0:
                            tempval = df.iloc[x-1,y-1]
                        else: tempval = 0
                    except:
                        tempval = 0

                    if y == 1:
                        val +=(tempval)
                    else: val +=(tempval/2)
                    try:
                        if y+1 <=max(df.columns):
                            tempval = df.iloc[x-1,y+1]
                        else: tempval = 0
                    except:
                        tempval = 0
                    if y == max(df.columns)-1:
                        val += tempval
                    else:           val +=(tempval/2)
                    df.iloc[x,y] = val
        last_row = df.iloc[-1,][df.iloc[-1,]!=0]
        return df, last_row


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
        pass

    def ii_init(self, record_info_in: object) -> bool:
        """
        Handles appending the new field to the incoming data.
        Called to report changes of the incoming connection's record metadata to the Alteryx engine.
        :param record_info_in: A RecordInfo object for the incoming connection's fields.
        :return: False if there's an error with the field name, otherwise True.
        """
        pass
    
    def ii_push_record(self, in_record: object) -> bool:
        """
        Responsible for pushing records out.
        Called when an input record is being sent to the plugin.
        :param in_record: The data for the incoming record.
        :return: False if there's a downstream error, or if there's an error with the field name, otherwise True.
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