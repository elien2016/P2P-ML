#!/usr/bin/env python3

import sys
import threading
import time
import tkinter
from collections import OrderedDict

import customtkinter

from btml import *

# Modes: 'System' (standard), 'Dark', 'Light'
customtkinter.set_appearance_mode('Dark')
# Themes: 'blue' (standard), 'green', 'dark-blue'
customtkinter.set_default_color_theme('blue')


class MLPeerGui(customtkinter.CTk):
    def __init__(self, max_peers, server_port, first_peer_ip, first_peer_port, auto_stabilize):
        super().__init__()

        self.commands = OrderedDict([('Query data', 'cluster-uri database "query"'), (
            'Query model', 'model-name ttl'), ('Connect and send', 'host port message-type "message-data"')])
        self.is_monitoring = False

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
        if self.peer_list.size() > 0:
            self.peer_list.delete(0, self.peer_list.size() - 1)

        for peer in self.mlpeer.getpeerids():
            self.peer_list.insert(tkinter.END, peer)

    def update_models(self):
        if self.model_list.size() > 0:
            self.model_list.delete(0, self.model_list.size() - 1)

        for model_name, (peerid, _, _) in self.mlpeer.model_map.items():
            self.model_list.insert(tkinter.END, '%s (%s)' %
                                   (model_name, peerid))

    def log_textbox_print(self, text):
        self.log_textbox.configure(state='normal')
        self.log_textbox.insert('end', '>> ' + str(text) + '\n')
        self.log_textbox.configure(state='disabled')

    def data_output_textbox_print(self, text):
        self.data_output_textbox.configure(state='normal')
        self.data_output_textbox.insert('end', '>> ' + str(text) + '\n')
        self.data_output_textbox.configure(state='disabled')

    def __infer(self, data):
        selections = self.model_list.curselection()
        if len(selections) == 1:
            model_name = self.model_list.get(selections[0]).split()[0]
            peerid, host, port = self.mlpeer.model_map[model_name]
            reply = self.mlpeer.connectandsend(
                host, port, 'INFR', '%s %s' % (model_name, data), peerid, True)
            self.log_textbox_print(reply)

    def __on_press_infer(self):
        dialog = customtkinter.CTkInputDialog(text='Data:', title='Infer')
        input = dialog.get_input()
        if input is None:
            return

        self.__infer(input)

    def __on_press_unload(self):
        selections = self.model_list.curselection()
        if len(selections) == 1:
            model_name = self.model_list.get(selections[0]).split()[0]
            self.mlpeer.unload_model(model_name)
            self.update_models()

    def __on_press_refresh(self):
        self.update_models()

    def __on_press_add(self):
        dialog = customtkinter.CTkInputDialog(
            text='peerid host port', title='Add peer')
        input = dialog.get_input()
        if input is None:
            return

        peer_info = input.split()
        if len(peer_info) == 3:
            try:
                peerid, host, port = peer_info
                port = int(port)
                message_data = '%s %s %d' % (
                    self.mlpeer.myid, self.mlpeer.serverhost, self.mlpeer.serverport)
                reply = self.mlpeer.connectandsend(
                    host, port, 'JOIN', message_data, peerid, True)

                if reply:
                    self.mlpeer.addpeer(peerid, host, port)
                    self.update_peers()
                elif self.mlpeer.debug:
                    self.log_textbox_print(
                        "Add peer: failed, peer not conforming to protocol")
            except:
                self.log_textbox_print("Add peer: invalid arguments")
        elif self.mlpeer.debug:
            self.log_textbox_print("Add peer: incorrect number of arguments")

    def __on_press_remove(self):
        selections = self.peer_list.curselection()
        if len(selections) == 1:
            peerid = self.peer_list.get(selections[0])
            self.mlpeer.sendtopeer(peerid, 'QUIT', self.mlpeer.myid)
            self.mlpeer.removepeer(peerid)
            self.update_peers()

    def __on_press_message(self):
        dialog = customtkinter.CTkInputDialog(
            text='message_type "message_data"', title='Message peer')
        input = dialog.get_input()
        if input is None:
            return

        message_input = input.split(maxsplit=1)
        selections = self.peer_list.curselection()
        if len(message_input) == 2 and len(selections) == 1:
            message_type, message_data = message_input
            message_input = message_input[1:-1]

            peerid = self.peer_list.get(selections[0])
            reply = self.mlpeer.sendtopeer(
                peerid, message_type, message_data, True)
            self.log_textbox_print(reply)

    def __on_press_stabilize(self):
        self.mlpeer.stabilize()
        self.update_peers()
        self.update_models()

    def __on_toggle_verbose(self):
        self.mlpeer.debug = self.verbose_switch.get()

    def __on_change_appearance_mode(self, new_appearance_mode):
        customtkinter.set_appearance_mode(new_appearance_mode)

    def __on_press_Azure_fetch_and_load(self):
        download_path = tkinter.filedialog.askdirectory(initialdir='.')
        if not download_path:
            return

        tenant_id = self.Azure_tenant_id_entry.get()
        subscription_id = self.Azure_subscription_id_entry.get()
        resource_group = self.Azure_resource_group_entry.get()
        workspace_name = self.Azure_workspace_name_entry.get()
        model_name = self.Azure_model_name_entry.get()
        model_version = self.Azure_model_version_entry.get() or None

        self.mlpeer.load_model_from_Azure_ML(
            tenant_id, subscription_id, resource_group, workspace_name, model_name, model_version, download_path)
        self.update_models()

    def __on_press_AWS_fetch_and_load(self):
        download_path = tkinter.filedialog.asksaveasfilename(initialdir='.')
        if not download_path:
            return

        access_key = self.AWS_access_key_entry.get()
        secret_key = self.AWS_secret_key_entry.get()
        region = self.AWS_region_entry.get()
        model_name = self.AWS_model_name_entry.get()

        self.mlpeer.load_model_from_AWS_SageMaker(
            access_key, secret_key, region, model_name, download_path)
        self.update_models()

    def __on_press_Local_load(self):
        path = tkinter.filedialog.askopenfilename(
            initialdir='.', filetypes=[('pickle', '.pkl .pickle')])
        if not path:
            return

        model_name = self.Local_model_name_entry.get()
        self.Local_model_name_entry.delete(0, len(model_name))

        self.mlpeer.load_model_from_path(model_name, path)
        self.update_models()

    def __on_press_start_monitoring(self):
        dialog = customtkinter.CTkInputDialog(
            text=self.commands['Query data'], title='Start monitoring')
        input = dialog.get_input()
        if input is None:
            return

        self.is_monitoring = True
        while self.is_monitoring:
            result = self.__query_data(input)
            infer_input = [
                list(float(str) if '.' in str else 0.0 for str in row) for row in result]
            self.__infer(infer_input)
            time.sleep(600)

    def __on_press_stop_monitoring(self):
        self.is_monitoring = False

    def __on_change_command(self, choice):
        self.command_arguments_entry.configure(
            placeholder_text=self.commands[choice])

    def __query_data(self, input: str):
        query_input = input.split(maxsplit=2)
        if len(query_input) == 3:
            cluster_uri, database, query = query_input
            query = query[1:-1]
            if not query:
                return

            return self.mlpeer.query_data_in_Azure_Data_Explorer(
                cluster_uri, database, query)

    def __on_press_execute(self):
        input = self.command_arguments_entry.get()
        self.command_arguments_entry.delete(0, len(input))
        if input is None:
            return

        match self.command_optionemenu.get():
            case "Query data":
                result = self.__query_data(input)
                for row in result:
                    self.data_output_textbox_print(row)
            case "Query model":
                query_input = input.split()
                if len(query_input) == 2:
                    model_name, ttl = query_input
                    try:
                        ttl = int(ttl)
                    except:
                        self.log_textbox_print("Query model: invalid ttl")
                        return

                    for peerid in self.mlpeer.getpeerids():
                        self.mlpeer.sendtopeer(
                            peerid, QUERY, "%s %s %d %s %d" % (self.mlpeer.myid, self.mlpeer.serverhost, self.mlpeer.serverport, model_name, ttl), self.mlpeer.debug)
            case "Connect and send":
                connect_and_send_input = input.split(maxsplit=3)
                if len(connect_and_send_input) == 4:
                    host, port, message_type, message_data = connect_and_send_input
                    message_data = message_data[1:-1]

                    reply = self.mlpeer.connectandsend(
                        host, port, message_type, message_data, None, True)
                    self.log_textbox_print(reply)

    def build_ui(self, server_port):
        # configure window
        self.title('P2P ML (%d)' % server_port)
        self.geometry(f'{1280}x{820}')

        # configure grid layout (3x3)
        self.grid_columnconfigure(1, weight=1)
        self.grid_rowconfigure((0, 1), weight=1)

        # create sidebar frame
        self.sidebar_frame = customtkinter.CTkFrame(
            self, width=190, corner_radius=0)
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
            self.model_list_frame, bg='old lace', bd=0, width=26, height=10)
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
            row=0, column=0, padx=(10, 0), pady=(0, 5), sticky='ns')
        self.model_unload_button = customtkinter.CTkButton(
            self.model_command_frame, width=100, border_width=2, fg_color='transparent', hover_color=('misty rose', 'black'), text_color=('gray10', '#DCE4EE'), text="Unload", command=self.__on_press_unload)
        self.model_unload_button.grid(
            row=0, column=1, padx=10, pady=(0, 5), sticky='ns')

        self.model_refresh_button = customtkinter.CTkButton(
            self.model_command_frame, width=210, border_width=2, fg_color='transparent', hover_color=('misty rose', 'black'), text_color=('gray10', '#DCE4EE'), text="Refresh", command=self.__on_press_refresh)
        self.model_refresh_button.grid(
            row=1, column=0, columnspan=2, padx=10, pady=(0, 5), sticky='ns')

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
            self.peer_list_frame, bg='azure2', bd=0, width=26, height=15)
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
            'Dark', 'Light', 'System'], command=self.__on_change_appearance_mode)
        self.appearance_mode_optionmenu.grid(row=1, column=0, pady=(10, 0))

        # create log textbox
        self.log_textbox = customtkinter.CTkTextbox(
            self, width=250)
        self.log_textbox.configure(state='disabled')
        self.log_textbox.grid(row=0, column=1, padx=(
            20, 0), pady=(20, 0), sticky='nsew')

        # create log textbox
        self.data_output_textbox = customtkinter.CTkTextbox(
            self, width=250)
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

        self.Azure_tenant_id_entry = customtkinter.CTkEntry(
            self.Azure_frame, width=320, placeholder_text='tenant id')
        self.Azure_tenant_id_entry.grid(
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
                                                                   'Azure Data Explorer'])
        self.data_source_optionemenu.grid(
            row=0, column=0, padx=20, pady=(20, 0))

        self.start_monitoring_button = customtkinter.CTkButton(
            self.data_source_frame, border_width=2, fg_color='transparent', hover_color=('misty rose', 'black'), text_color=('gray10', '#DCE4EE'), text="Start monitoring", command=self.__on_press_start_monitoring)
        self.start_monitoring_button.grid(
            row=1, column=0, padx=20, pady=(15, 0), sticky='ns')

        self.stop_monitoring_button = customtkinter.CTkButton(
            self.data_source_frame, border_width=2, fg_color='transparent', hover_color=('misty rose', 'black'), text_color=('gray10', '#DCE4EE'), text="Stop monitoring", command=self.__on_press_stop_monitoring)
        self.stop_monitoring_button.grid(
            row=2, column=0, padx=20, pady=10, sticky='ns')

        # create command frame
        self.command_frame = customtkinter.CTkFrame(self)
        self.command_frame.grid(
            row=2, column=1, columnspan=2, padx=20, pady=20, sticky='nsew')
        self.command_frame.grid_columnconfigure(0, weight=1)

        self.command_arguments_entry = customtkinter.CTkEntry(
            self.command_frame, placeholder_text=list(self.commands.values())[0])
        self.command_arguments_entry.grid(
            row=0, column=0, padx=(20, 0), pady=15, sticky='nsew')

        self.command_operations_frame = customtkinter.CTkFrame(
            self.command_frame, fg_color='transparent', width=360)
        self.command_operations_frame.grid(
            row=0, column=1, padx=20, pady=15, sticky='nsew')
        self.command_operations_frame.grid_columnconfigure((0, 1), weight=1)

        self.command_optionemenu = customtkinter.CTkOptionMenu(
            self.command_operations_frame, values=list(self.commands.keys()), command=self.__on_change_command)
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
