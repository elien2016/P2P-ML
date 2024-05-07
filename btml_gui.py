#!/usr/bin/env python3

import sys
import threading
import tkinter

import customtkinter

from btml import MLPeer

# Modes: 'System' (standard), 'Dark', 'Light'
customtkinter.set_appearance_mode('Dark')
# Themes: 'blue' (standard), 'green', 'dark-blue'
customtkinter.set_default_color_theme('blue')


class MLPeerGui(customtkinter.CTk):
    def __init__(self, max_peers, server_port, first_peer_ip, first_peer_port, auto_stabilize):
        super().__init__()

        self.debug = 0

        self.build_ui(server_port)

        self.mlpeer = MLPeer(max_peers, server_port)

        t_server = threading.Thread(
            target=self.mlpeer.mainloop, args=[], daemon=True)
        t_server.start()

        self.mlpeer.buildpeers(first_peer_ip, first_peer_port)
        self.update_peers()

        if auto_stabilize:
            self.mlpeer.startstabilizer(self.mlpeer.stabilize, 10)

    def update_peers(self):
        # TODO
        pass

    def update_models(self):
        # TODO
        pass

    def open_input_dialog_event(self):
        dialog = customtkinter.CTkInputDialog(
            text='Type in a number:', title='CTkInputDialog')
        print('CTkInputDialog:', dialog.get_input())

    def change_appearance_mode_event(self, new_appearance_mode: str):
        customtkinter.set_appearance_mode(new_appearance_mode)

    def change_scaling_event(self, new_scaling: str):
        new_scaling_float = int(new_scaling.replace('%', '')) / 100
        customtkinter.set_widget_scaling(new_scaling_float)

    def sidebar_button_event(self):
        print('sidebar_button click')

    def __on_toggle_verbose(self):
        self.debug = self.verbose_switch.get()

    def __on_press_infer(self):
        # TODO
        pass

    def __on_press_unload(self):
        # TODO
        pass

    def __on_press_add(self):
        # TODO
        pass

    def __on_press_remove(self):
        # TODO
        pass

    def __on_press_message(self):
        # TODO
        pass

    def __on_press_stabilize(self):
        # TODO
        pass

    def __on_press_Azure_fetch_and_load(self):
        print(tkinter.filedialog.askdirectory(initialdir='.'))

    def __on_press_AWS_fetch_and_load(self):
        print(tkinter.filedialog.asksaveasfilename(initialdir='.'))

    def __on_press_Local_load(self):
        print(tkinter.filedialog.askopenfilename(
            initialdir='.', filetypes=[('pickle', '.pkl .pickle')]))

    def __on_change_data_source(self, choice):
        # TODO
        pass

    def __on_press_start_monitoring(self):
        # TODO
        pass

    def __on_change_command(self, choice):
        # TODO
        pass

    def __on_press_execute(self):
        # TODO
        pass

    def build_ui(self, server_port):
        # configure window
        self.title('P2P ML (%d)' % server_port)
        self.geometry(f'{1250}x{785}')

        # configure grid layout (3x3)
        self.grid_columnconfigure(1, weight=1)
        self.grid_rowconfigure((0, 1), weight=1)

        # create sidebar frame
        self.sidebar_frame = customtkinter.CTkFrame(
            self, width=180, corner_radius=0)
        self.sidebar_frame.grid(row=0, column=0, rowspan=3, sticky='nsew')
        self.sidebar_frame.grid_rowconfigure((0, 1), weight=1)

        # create models frame
        self.models_frame = customtkinter.CTkFrame(
            self.sidebar_frame, fg_color='transparent')
        self.models_frame.grid(row=0, column=0, padx=10,
                               pady=(10, 0), sticky='nsew')

        self.model_list_label = customtkinter.CTkLabel(
            self.models_frame, text='Models', font=customtkinter.CTkFont(size=14, weight='bold'))
        self.model_list_label.grid(row=0, column=0, pady=(0, 5))

        self.model_list_frame = customtkinter.CTkFrame(self.models_frame)
        self.model_list_frame.grid(row=1, column=0)

        self.model_list = tkinter.Listbox(
            self.model_list_frame, bg='old lace', bd=0, width=22, height=10)
        self.model_list.insert(tkinter.END, 'a', 'b', 'c',
                               'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o')
        self.model_list.grid(row=0, column=0, padx=(
            10, 0), pady=(0, 10), sticky='ns')

        self.model_list_scrollbar = customtkinter.CTkScrollbar(
            self.model_list_frame, command=self.model_list.yview)
        self.model_list_scrollbar.grid(row=0, column=1, sticky='ns')

        self.model_list.configure(yscrollcommand=self.model_list_scrollbar.set)

        self.model_command_frame = customtkinter.CTkFrame(
            self.models_frame, fg_color='transparent')
        self.model_command_frame.grid(row=2, column=0)

        self.model_infer_button = customtkinter.CTkButton(
            self.model_command_frame, width=100, border_width=2, fg_color='transparent', hover_color=('misty rose', 'black'), text_color=('gray10', '#DCE4EE'), text="Infer", command=self.__on_press_infer)
        self.model_infer_button.grid(
            row=0, column=0, padx=(10, 0), sticky='ns')
        self.model_unload_button = customtkinter.CTkButton(
            self.model_command_frame, width=100, border_width=2, fg_color='transparent', hover_color=('misty rose', 'black'), text_color=('gray10', '#DCE4EE'), text="Unload", command=self.__on_press_unload)
        self.model_unload_button.grid(row=0, column=1, padx=10, sticky='ns')

        # create peers frame
        self.peers_frame = customtkinter.CTkFrame(
            self.sidebar_frame, fg_color='transparent')
        self.peers_frame.grid(row=1, column=0, padx=10,
                              pady=(5, 0), sticky='nsew')

        self.peer_list_label = customtkinter.CTkLabel(
            self.peers_frame, text='Peers', font=customtkinter.CTkFont(size=14, weight='bold'))
        self.peer_list_label.grid(row=0, column=0, pady=(0, 5))

        self.peer_list_frame = customtkinter.CTkFrame(self.peers_frame)
        self.peer_list_frame.grid(row=1, column=0)

        self.peer_list = tkinter.Listbox(
            self.peer_list_frame, bg='azure2', bd=0, width=22, height=15)
        self.peer_list.insert(tkinter.END, 'a', 'b', 'c',
                              'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v')
        self.peer_list.grid(row=0, column=0, padx=(10, 0),
                            pady=(0, 10), sticky='ns')

        self.peer_list_scrollbar = customtkinter.CTkScrollbar(
            self.peer_list_frame, command=self.peer_list.yview)
        self.peer_list_scrollbar.grid(row=0, column=1, sticky='ns')

        self.peer_list.configure(yscrollcommand=self.peer_list_scrollbar.set)

        self.peer_command_frame = customtkinter.CTkFrame(
            self.peers_frame, fg_color='transparent')
        self.peer_command_frame.grid(row=2, column=0)

        self.peer_add_button = customtkinter.CTkButton(
            self.peer_command_frame, width=100, border_width=2, fg_color='transparent', hover_color=('misty rose', 'black'), text_color=('gray10', '#DCE4EE'), text="Add", command=self.__on_press_add)
        self.peer_add_button.grid(
            row=0, column=0, padx=(10, 0), pady=(0, 5), sticky='ns')
        self.peer_remove_button = customtkinter.CTkButton(
            self.peer_command_frame, width=100, border_width=2, fg_color='transparent', hover_color=('misty rose', 'black'), text_color=('gray10', '#DCE4EE'), text="Remove", command=self.__on_press_remove)
        self.peer_remove_button.grid(
            row=0, column=1, padx=10, pady=(0, 5), sticky='ns')

        self.peer_message_button = customtkinter.CTkButton(
            self.peer_command_frame, width=210, border_width=2, fg_color='transparent', hover_color=('misty rose', 'black'), text_color=('gray10', '#DCE4EE'), text="Message", command=self.__on_press_message)
        self.peer_message_button.grid(
            row=1, column=0, columnspan=2, padx=10, pady=(0, 5), sticky='ns')

        self.stabilize_button = customtkinter.CTkButton(
            self.peer_command_frame, width=210, border_width=2, fg_color='transparent', hover_color=('misty rose', 'black'), text_color=('gray10', '#DCE4EE'), text="Stabilize", command=self.__on_press_stabilize)
        self.stabilize_button.grid(
            row=2, column=0, columnspan=2, padx=10, pady=(0, 5), sticky='ns')

        # create settings frame
        self.settings_frame = customtkinter.CTkFrame(
            self.sidebar_frame, fg_color='transparent')
        self.settings_frame.grid(row=2, column=0, padx=(
            20, 20), pady=20, sticky='nsew')
        self.settings_frame.grid_columnconfigure(0, weight=1)

        self.verbose_switch = customtkinter.CTkSwitch(
            self.settings_frame, text='Verbose', command=self.__on_toggle_verbose)
        self.verbose_switch.deselect()
        self.verbose_switch.grid(row=0, column=0, pady=(0, 5))

        self.appearance_mode_optionmenu = customtkinter.CTkOptionMenu(self.settings_frame, values=[
            'Dark', 'Light', 'System'], command=self.change_appearance_mode_event)
        self.appearance_mode_optionmenu.grid(row=1, column=0, pady=(10, 0))

        # create log textbox
        self.log_textbox = customtkinter.CTkTextbox(
            self, width=250)
        self.log_textbox.insert('0.0', 'CTkTextbox\n\n' +
                                'Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, sed diam voluptua.\n\n' * 20)
        self.log_textbox.configure(state='disabled')
        self.log_textbox.grid(row=0, column=1, padx=(
            20, 0), pady=(20, 0), sticky='nsew')

        # create log textbox
        self.data_output_textbox = customtkinter.CTkTextbox(
            self, width=250)
        self.data_output_textbox.insert('0.0', 'CTkTextbox\n\n' +
                                        'Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, sed diam voluptua.\n\n' * 20)
        self.data_output_textbox.configure(state='disabled')
        self.data_output_textbox.grid(
            row=1, column=1, padx=(20, 0), pady=(20, 0), sticky='nsew')

        # create model loader tabview
        self.model_loader_tabview = customtkinter.CTkTabview(self, width=360)
        self.model_loader_tabview.grid(
            row=0, column=2, padx=20, pady=(20, 0), sticky='nsew')
        self.model_loader_tabview.add('Azure')
        self.model_loader_tabview.add('AWS')
        self.model_loader_tabview.add('Local')

        self.model_loader_tabview.tab(
            'Azure').grid_rowconfigure(0, weight=1)
        self.model_loader_tabview.tab(
            'AWS').grid_rowconfigure(0, weight=1)
        self.model_loader_tabview.tab(
            'Azure').grid_columnconfigure(0, weight=1)
        self.model_loader_tabview.tab(
            'AWS').grid_columnconfigure(0, weight=1)
        self.model_loader_tabview.tab(
            'Local').grid_columnconfigure(0, weight=1)

        self.Azure_frame = customtkinter.CTkScrollableFrame(
            self.model_loader_tabview.tab('Azure'))
        self.Azure_frame.grid(row=0, column=0, sticky='nswe')
        self.Azure_frame.grid_columnconfigure(0, weight=1)

        self.Azure_tenand_id_entry = customtkinter.CTkEntry(
            self.Azure_frame, width=320, placeholder_text='tenand id')
        self.Azure_tenand_id_entry.grid(
            row=0, column=0, pady=(10, 0))
        self.Azure_subscription_id_entry = customtkinter.CTkEntry(
            self.Azure_frame, width=320, placeholder_text='subscription id')
        self.Azure_subscription_id_entry.grid(
            row=1, column=0, pady=(10, 0))
        self.Azure_resource_group_entry = customtkinter.CTkEntry(
            self.Azure_frame, width=320, placeholder_text='resource group')
        self.Azure_resource_group_entry.grid(
            row=2, column=0, pady=(10, 0))
        self.Azure_workspace_name_entry = customtkinter.CTkEntry(
            self.Azure_frame, width=320, placeholder_text='workspace name')
        self.Azure_workspace_name_entry.grid(
            row=3, column=0, pady=(10, 0))
        self.Azure_model_name_entry = customtkinter.CTkEntry(
            self.Azure_frame, width=320, placeholder_text='model name')
        self.Azure_model_name_entry.grid(
            row=4, column=0, pady=(10, 0))
        self.Azure_model_version_entry = customtkinter.CTkEntry(
            self.Azure_frame, width=320, placeholder_text='model version (optional)')
        self.Azure_model_version_entry.grid(
            row=5, column=0, pady=(10, 0))

        self.Azure_fetch_button = customtkinter.CTkButton(
            self.Azure_frame, width=320, text='Fetch and load', command=self.__on_press_Azure_fetch_and_load)
        self.Azure_fetch_button.grid(row=6, column=0, pady=10)

        self.AWS_frame = customtkinter.CTkScrollableFrame(
            self.model_loader_tabview.tab('AWS'))
        self.AWS_frame.grid(row=0, column=0, sticky='nswe')
        self.AWS_frame.grid_columnconfigure(0, weight=1)

        self.AWS_access_key_entry = customtkinter.CTkEntry(
            self.AWS_frame, width=320, placeholder_text='access key')
        self.AWS_access_key_entry.grid(
            row=0, column=0, pady=(10, 0))
        self.AWS_secret_key_entry = customtkinter.CTkEntry(
            self.AWS_frame, width=320, placeholder_text='secret key')
        self.AWS_secret_key_entry.grid(
            row=1, column=0, pady=(10, 0))
        self.AWS_region_entry = customtkinter.CTkEntry(
            self.AWS_frame, width=320, placeholder_text='region')
        self.AWS_region_entry.grid(
            row=2, column=0, pady=(10, 0))
        self.AWS_model_name_entry = customtkinter.CTkEntry(
            self.AWS_frame, width=320, placeholder_text='model name')
        self.AWS_model_name_entry.grid(
            row=3, column=0, pady=(10, 0))

        self.AWS_fetch_button = customtkinter.CTkButton(
            self.AWS_frame, width=320, text='Fetch and load', command=self.__on_press_AWS_fetch_and_load)
        self.AWS_fetch_button.grid(row=4, column=0, pady=10)

        self.Local_model_name_entry = customtkinter.CTkEntry(
            self.model_loader_tabview.tab(
                'Local'), width=320, placeholder_text='model name')
        self.Local_model_name_entry.grid(
            row=0, column=0, pady=(10, 0))

        self.Local_load_button = customtkinter.CTkButton(
            self.model_loader_tabview.tab(
                'Local'), width=320, text='Load', command=self.__on_press_Local_load)
        self.Local_load_button.grid(row=1, column=0, pady=10)

        # create data source frame
        self.data_source_frame = customtkinter.CTkFrame(self, width=360)
        self.data_source_frame.grid(row=1, column=2, padx=(
            20, 20), pady=(20, 0), sticky='nsew')
        self.data_source_frame.grid_columnconfigure(0, weight=1)

        self.data_source_optionemenu = customtkinter.CTkOptionMenu(self.data_source_frame, values=[
                                                                   'Azure Data Explorer'], command=self.__on_change_data_source)
        self.data_source_optionemenu.grid(
            row=0, column=0, padx=20, pady=(20, 0))

        self.start_monitoring_button = customtkinter.CTkButton(
            self.data_source_frame, border_width=2, fg_color='transparent', hover_color=('misty rose', 'black'), text_color=('gray10', '#DCE4EE'), text="Start monitoring", command=self.__on_press_start_monitoring)
        self.start_monitoring_button.grid(
            row=1, column=0, padx=20, pady=15, sticky='ns')

        # create command frame
        self.command_frame = customtkinter.CTkFrame(self)
        self.command_frame.grid(
            row=2, column=1, columnspan=2, padx=20, pady=20, sticky='nsew')
        self.command_frame.grid_columnconfigure(0, weight=1)

        self.command_arguments_entry = customtkinter.CTkEntry(
            self.command_frame, placeholder_text='arguments')
        self.command_arguments_entry.grid(
            row=0, column=0, padx=(20, 0), pady=15, sticky='nsew')

        self.command_operations_frame = customtkinter.CTkFrame(
            self.command_frame, fg_color='transparent', width=360)
        self.command_operations_frame.grid(
            row=0, column=1, padx=20, pady=15, sticky='nsew')
        self.command_operations_frame.grid_columnconfigure((0, 1), weight=1)

        self.command_optionemenu = customtkinter.CTkOptionMenu(self.command_operations_frame, values=[
            'Query data', 'Connect and send'], command=self.__on_change_command)
        self.command_optionemenu.grid(row=0, column=0)

        self.execute_button = customtkinter.CTkButton(
            self.command_operations_frame, border_width=2, fg_color='transparent', hover_color=('misty rose', 'black'), text_color=('gray10', '#DCE4EE'), text="Execute", command=self.__on_press_execute)
        self.execute_button.grid(
            row=0, column=1, padx=(20, 0), sticky='ns')


if __name__ == '__main__':
    if len(sys.argv) < 5 or len(sys.argv[3].split(':')) != 2:
        print(
            'Usage: %s max-peers server-port first-peer-ip:first-peer-port auto-stabilize' % sys.argv[0])
        sys.exit(1)

    try:
        max_peers = int(sys.argv[1])
        server_port = int(sys.argv[2])

        first_peer_ip, first_peer_port = sys.argv[3].split(':')
        int(first_peer_port)

        auto_stabilize = int(sys.argv[4])
        if auto_stabilize not in [0, 1]:
            raise
    except:
        print('Invalid arguments')
        print(
            'Usage: %s max-peers server-port first-peer-ip:first-peer-port auto-stabilize' % sys.argv[0])
        sys.exit(1)

    app = MLPeerGui(max_peers, server_port, first_peer_ip,
                    first_peer_port, auto_stabilize)
    app.mainloop()
