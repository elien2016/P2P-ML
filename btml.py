#!/usr/bin/env python3

import json
import os
import pickle

from azure.ai.ml import MLClient
from azure.identity import InteractiveBrowserCredential
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from sklearn import *

from btpeer import *

PEERNAME = "NAME"   # request a peer's canonical id
LISTPEERS = "LIST"
INSERTPEER = "JOIN"
QUERY = "QUER"
QRESPONSE = "RESP"
INFER = "INFR"
PEERQUIT = "QUIT"

REPLY = "REPL"
ERROR = "ERRO"


class MLPeer(BTPeer):
    """
    Implements a peer in a Machine Learning inference network based on the
    generic BerryTella P2P framework.
    """

    def __init__(self, maxpeers, serverport, myid=None, serverhost=None):
        """
        Initializes the peer to support connections up to maxpeers number
        of peers, with its server listening on the specified port. Also sets
        the dictionary of models to empty and adds handlers to the
        BTPeer framework.
        """
        BTPeer.__init__(self, maxpeers, serverport, myid, serverhost)

        # modelname --> model mapping
        self.models = {}

        # modelname --> (peerid, host, port) mapping
        self.model_registry = {}

        self.addrouter(self.__router)

        self.addhandler(PEERNAME, self.__handle_peername)
        self.addhandler(LISTPEERS, self.__handle_listpeers)
        self.addhandler(INSERTPEER, self.__handle_insertpeer)
        self.addhandler(QUERY, self.__handle_query)
        self.addhandler(QRESPONSE, self.__handle_qresponse)
        self.addhandler(INFER, self.__handle_infer)
        self.addhandler(PEERQUIT, self.__handle_quit)

    def __debug(self, msg):
        if self.debug:
            btdebug(msg)

    def __router(self, peerid):
        if peerid not in self.getpeerids():
            return (None, None, None)
        else:
            host, port = self.peers[peerid]
            return (peerid, host, port)

    def __stabilize(self):
        todelete = self.checklivepeers()

        self.peerlock.acquire()
        try:
            for peerid in todelete:
                self.removepeer(peerid)
        finally:
            self.peerlock.release()

    def __handle_peername(self, peerconn, data):
        """Handles the NAME message type. Message data is not used."""
        peerconn.senddata(REPLY, self.myid)

    def __handle_listpeers(self, peerconn, data):
        """Handles the LISTPEERS message type. Message data is not used."""

        self.peerlock.acquire()
        try:
            self.__debug('Listing peers, total %d' % self.numberofpeers())
            peerconn.senddata(REPLY, '%d' % self.numberofpeers())
            for peerid in self.getpeerids():
                host, port = self.peers[peerid]
                peerconn.senddata(REPLY, '%s %s %d' % (peerid, host, port))
        finally:
            self.peerlock.release()

    def __handle_insertpeer(self, peerconn, data):
        """
        Handles the INSERTPEER (join) message type. The message data
        should be a string of the form, "peerid host port", where peerid
        is the canonical name of the peer that desires to be added to this
        peer's list of peers, host and port are the necessary data to connect
        to the peer.
        """

        if self.maxpeersreached():
            self.__debug(
                'maxpeers %d reached: connection terminating' % self.maxpeers)
            peerconn.senddata(ERROR, 'Join: too many peers')
            return

        try:
            peerid, host, port = data.split()
        except:
            self.__debug('invalid insert %s: %s' % (str(peerconn), data))
            peerconn.senddata(ERROR, 'Join: incorrect arguments')
            return

        self.peerlock.acquire()
        try:
            if self.addpeer(peerid, host, port):
                self.__debug('added peer: %s' % peerid)
                peerconn.senddata(REPLY, 'Join: peer added: %s' % peerid)
            else:
                peerconn.senddata(
                    ERROR, 'Join: peer already inserted or is self %s' % peerid)
        finally:
            self.peerlock.release()

    def __handle_query(self, peerconn, data):
        """
        Handles the QUERY message type. The message data should be in the
        format of a string, "return-peer-id return-peer-host return-peer-port
        modelname ttl", where return-peer-id is the name of the peer that
        initiated the query, modelname is the name of the model
        being searched for, and ttl is how many further levels of peers
        this query should be propagated on.
        """

        try:
            peerid, host, port, modelname, ttl = data.split()
        except:
            self.__debug('invalid query %s: %s' % (str(peerconn), data))
            peerconn.senddata(ERROR, 'Query: incorrect arguments')
            return

        peerconn.senddata(REPLY, 'Query ACK: %s' % modelname)

        t = threading.Thread(target=self.__processquery,
                             args=[peerid, host, port, modelname, int(ttl)])
        t.start()

    def __processquery(self, peerid, host, port, modelname, ttl):
        """
        Handles the processing of a query message after it has been
        received and acknowledged, by either replying with a QRESPONSE message
        if the model is in self.model_registry, or propagating the message onto
        all immediate neighbors.
        """

        if modelname in self.model_registry:
            mpeerid, mpeerhost, mpeerport = self.model_registry[modelname]
            if mpeerid is None:     # own models mapped to None
                mpeerid = self.myid

            # can't use sendtopeer here because peerid is not necessarily
            # an immediate neighbor
            self.connectandsend(host, port, QRESPONSE, '%s %s %s %s' % (
                modelname, mpeerid, mpeerhost, mpeerport), peerid, False)
            return

        # will only reach here if modelname not found... in which case
        # propagate query to neighbors
        if ttl > 0:
            msgdata = '%s %s %s %s %d' % (
                peerid, host, port, modelname, ttl - 1)
            for nextpeerid in self.getpeerids():
                if nextpeerid != peerid:
                    self.sendtopeer(nextpeerid, QUERY, msgdata)

    def __handle_qresponse(self, peerconn, data):
        """
        Handles the QRESPONSE message type. The message data should be
        in the format of a string, "modelname peerid host port",
        where modelname is the model that was queried about and peerid is
        the name of a peer capable of serving inference for that model.
        """
        try:
            modelname, mpeerid, host, port = data.split()
        except:
            self.__debug('invalid qresponse %s: %s' % (str(peerconn), data))
            peerconn.senddata(ERROR, 'Resp: incorrect arguments')
            return

        if modelname in self.model_registry:
            self.__debug("can't add duplicate model %s %s" %
                         (modelname, mpeerid))
        else:
            self.model_registry[modelname] = (mpeerid, host, int(port))

    def __handle_infer(self, peerconn, data):
        """
        Handles the INFER message type. The message data should be in
        the format of a string, "modelname input", where modelname is the name
        of the model to be used.
        """

        try:
            modelname, input = data.split()
        except:
            self.__debug('invalid infer %s: %s' % (str(peerconn), data))
            peerconn.senddata(ERROR, 'Infr: incorrect arguments')
            return

        if modelname not in self.models:
            self.__debug('model not found %s' % modelname)
            peerconn.senddata(ERROR, 'Model not found')
            return

        try:
            model = self.models[modelname]
            X = json.loads(input)
            Y_pred = model.predict(X)
            output = json.dumps(Y_pred.tolist())
        except Exception as e:
            peerconn.senddata(ERROR, 'Error running inference: %s' % type(e))
            if self.debug:
                traceback.print_exc()
            return

        peerconn.senddata(REPLY, output)

    def __handle_quit(self, peerconn, data):
        """
        Handles the QUIT message type. The message data should be in the
        format of a string, "peerid", where peerid is the canonical
        name of the peer that wishes to be unregistered from this
        peer's directory.
        """

        self.peerlock.acquire()
        try:
            peerid = data
            if peerid in self.getpeerids():
                self.removepeer(peerid)
                msg = 'Quit: peer removed: %s' % peerid
                self.__debug(msg)
                peerconn.senddata(REPLY, msg)
            else:
                msg = 'Quit: peer not found: %s' % peerid
                self.__debug(msg)
                peerconn.senddata(ERROR, msg)
        finally:
            self.peerlock.release()

    # precondition: may be a good idea to hold the lock before going
    #               into this function
    def buildpeers(self, host, port, hops=1):
        """
        buildpeers(host, port, hops)

        Attempts to build the local peer list up to the limit stored by
        self.maxpeers, using a simple depth-first search given an
        initial host and port as starting point. The depth of the
        search is limited by the hops parameter.
        """

        if self.maxpeersreached() or not hops:
            return

        peerid = None

        self.__debug("Building peers from (%s,%s)" % (host, port))

        try:
            _, peerid = self.connectandsend(host, port, PEERNAME, '')[0]

            self.__debug("contacted " + peerid)
            onereply = self.connectandsend(host, port, INSERTPEER, '%s %s %d' % (
                self.myid, self.serverhost, self.serverport), peerid)[0]

            self.__debug(str(onereply))
            if (onereply[0] != REPLY) or (peerid in self.getpeerids()):
                return

            self.addpeer(peerid, host, port)

            if not self.maxpeersreached() and hops > 1:
                # do recursive depth first search to add more peers
                msgreply = self.connectandsend(
                    host, port, LISTPEERS, '', peerid)
                if len(msgreply) > 1:
                    for reply in msgreply[1:]:  # get rid of header count reply
                        nextpeerid, nextpeerhost, nextpeerport = reply[1].split(
                        )
                        if nextpeerid != self.myid:
                            self.buildpeers(
                                nextpeerhost, nextpeerport, hops - 1)
                            if self.maxpeersreached():
                                return
        except:
            if self.debug:
                traceback.print_exc()
            self.removepeer(peerid)

    def load_model_from_path(self, model_name, model_dir):
        """Loads a model from a pickle file."""

        model_path = None
        for file in os.listdir(model_dir):
            if file.endswith(".pkl") or file.endswith(".pickle"):
                model_path = os.path.join(model_dir, file)
                break

        if model_path is None:
            self.__debug(
                "no .pkl or .pickle file found in %s" % model_dir)

        try:
            with open(model_path, 'rb') as f:
                self.models[model_name] = pickle.load(f)

            self.model_registry[model_name] = (
                None, self.serverhost, self.serverport)
        except pickle.UnpicklingError:
            self.__debug("error loading model from %s" % model_path)

    def load_model_from_Azure_ML(self, tenant_id, subscription_id, resource_group, workspace_name, model_name, model_version=None, download_path='.'):
        """Loads a model from Azure Machine Learning."""

        try:
            ws = MLClient(InteractiveBrowserCredential(tenant_id=tenant_id),
                          subscription_id, resource_group, workspace_name)

            if model_version is None:
                model_version = max(    # get the latest model version
                    [int(m.version) for m in ws.models.list(name=model_name)]
                )

            ws.models.download(model_name, model_version, download_path)

            self.load_model_from_path(
                model_name, os.path.join(download_path, model_name, ws.models.get(
                    model_name, model_version).path.split('/')[-2]))
            self.__debug("loaded model %s %d from Azure ML" %
                         model_name, model_version)
        except:
            if self.debug:
                traceback.print_exc()

    def load_model_from_AWS_SageMaker(self, model_name):
        """Loads a model from AWS SageMaker."""

        # TODO implement model loading from AWS SageMaker

        import sagemaker, boto3, pathlib

        pathlib.Path("./output").mkdir(parents=True, exist_ok=True)

        bucket = "sagemaker-us-east-2-851725545677"

        file = open('./credentials.txt', 'r')
        access_key_id = file.readline().replace('\n', '')
        secret_access_key = file.readline().replace('\n', '')
        file.close()

        boto3.Session().resource('s3', aws_access_key_id=access_key_id,
                 aws_secret_access_key=secret_access_key).Bucket(bucket).Object(
            'eruption-prediction/output/lgb_model.pkl').download_file('./output/lgb_model.pkl')

        self.load_model_from_path(
            model_name, os.path.join(download_path, model_name, './output/lgb_model.pkl')
        )
        

        self.model_registry[model_name] = (
            None, self.serverhost, self.serverport)
        self.__debug("loaded model %s from AWS SageMaker" % model_name)

    def unload_model(self, model_name):
        """Unloads a model."""

        if model_name in self.models:
            del self.models[model_name]

        if model_name in self.model_registry:
            del self.model_registry[model_name]

    def query_data_in_Azure_Data_Explorer(self, cluster_uri, database, query):
        """Sends the query to Azure Data Explorer."""

        try:
            kcsb = KustoConnectionStringBuilder.with_interactive_login(
                cluster_uri)

            self.__debug("Querying data from Azure Data Explorer")
            with KustoClient(kcsb) as kusto_client:
                response = kusto_client.execute(database, query)

                for row in response.primary_results[0]:
                    self.__debug(row)
        except:
            if self.debug:
                traceback.print_exc()
