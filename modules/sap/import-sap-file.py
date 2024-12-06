# module required for framework integration
from recon.core.module import BaseModule
# mixins for desired functionality
from recon.mixins.resolver import ResolverMixin
from recon.mixins.threads import ThreadingMixin
# module specific imports
import os, pdb
import re
from lxml import etree
import os
import sys
import json
from kysecc.helpers.sqlite_helpers import SQLHelper
from kysecc.helpers.sqlite_helpers import INSTANCE_TYPE_DIALOG, INSTANCE_TYPE_MSGSERVER, INSTANCE_TYPE_BOTH

class Module(BaseModule, ResolverMixin, ThreadingMixin):

    # modules are defined and configured by the "meta" class variable
    # "meta" is a dictionary that contains information about the module, ranging from basic information, to input that affects how the module functions
    # below is an example "meta" declaration that contains all of the possible definitions

    meta = {
        'name': 'Import SAP file',
        'author': 'Kai Ullrich',
        'version': '0.1',
        'description': 'Imports discovery file into the DB',
        'options': (
            ('inputfile', 'no default', 'yes', 'file with the input to be imported'),
            ('inputtype', 'SYSINFO', 'yes', """One of:
SYSINFO (output of the sysinfo sweep)
LANDSCAPE_XML (SAPUILandscapeCentral.xml)
SAPWNGUIXS (Output of appservers)""")
        )
        
    }

    def parse_sap_document (self, filename):

        server_dict = {}
        server_elem = {}

        document = etree.parse (filename)

        for elem in document.iter ():
            if elem.tag == 'Messageserver':
                uuid = elem.get ('uuid')
                if uuid not in server_dict:
                    server_elem = {}
                    server_dict [uuid] = server_elem
                else:
                    server_elem = server_dict [uuid]

                server_elem ['uuid'] = uuid
                server_elem ['name'] = elem.get ('name')
                server_elem ['host'] = elem.get ('host')
                server_elem ['port'] = elem.get ('port')
                server_elem ['type'] = 'msgserver'
                
                if elem.get ('description') is not None:
                    server_elem ['description'] = elem.get ('description')

            elif elem.tag == 'Service':
                uuid = elem.get ('msid')
                if uuid is None:
                    uuid = elem.get ('uuid')
                if uuid not in server_dict:
                    server_elem = {}
                    server_dict [uuid] = server_elem
                else:
                    server_elem = server_dict [uuid]

                server_elem ['description'] = elem.get ('name')
                server = elem.get ('server')
                p = re.compile ('([^:]*):([1-9][0-9]{3,4})')
                g = p.match (server)
                if g is not None:
                    server_elem ['type'] = 'dialog'
                    server_elem ['host'] = g.group (1)
                    server_elem ['port'] = g.group (2)
                    server_elem ['name'] = elem.get ('systemid')
            else:
                continue
        
        return server_dict
    
    # inserts or updates records in the SYSTEMS or the INSTANCES table
    # server_dict consists of the following elements:
    # 
    # o type 'appserver' or 'msgserver'
    # o name SYSID of the system
    # o host hostname (either msgserver or appserver, depending on 'type')
    # o port port of the connection. The SYSNR is going to be computed by port % 100
    # o description available description of the server.
    # 
    # 
    def insert_or_update_system_and_server (self, server_dict):
        sysid = server_dict ['name']
        #description = server_dict ['description'] if 'description' in server_dict else ''
        if 'description' not in server_dict or server_dict['description'] is None:
            description = ''
        else:
            description = server_dict ['description']
        if 'host' not in server_dict or server_dict['host'] is None:
            #
            # Einträge ohne Host ignorieren wir mal geflissentlich
            #
            sys.stdout.write ("-")
            return
        else:
            host = server_dict['host']

        #
        # check if we need to insert IP or hostname
        #
        p = re.compile ("[1-9][0-9]{0,2}\.[1-9][0-9]{0,2}\.[1-9][0-9]{0,2}\.[1-9][0-9]{0,2}")
        m = p.match (host)
        is_ip = m is not None


        dbh = SQLHelper (self)
        dbh.openconnection ()
        b_commit = True

        try:
            # insert the system
            rowid_system = dbh.insert_or_get_rowid_systems (sysid, description)

            if is_ip:
                rowid_hosts = dbh.insert_or_get_rowid_hosts (host)
            else:
                rowid_hosts = dbh.insert_or_get_rowid_hosts (None, host)

            #
            # now insert the appserver
            #

            # first do SYSNR
            port = server_dict ['port']
            sysnr = int(port) % 100
            type = server_dict ['type']

            # then do the INSERT
            dbh.insert_or_update_instance (rowid_system, rowid_hosts, type, port, sysnr)
        except Exception as e:
            self.error ('Exception happened with system {}, description {}, host {}'.format (sysid, description, host))
            b_commit = False
        finally:
            dbh.closeconnection (b_commit)
        


    def module_run(self):
 
        filename = self.options.get ('INPUTFILE')
        if not os.path.exists (filename) or not os.path.isfile (filename):
            self.error ("File {} does not exist".format (filename))
            return
        
        inputtype = self.options.get ('INPUTTYPE')

        if inputtype == 'LANDSCAPE_XML': 

            server_dict = self.parse_sap_document (filename)
            for key in server_dict:
                try:
                    self.insert_or_update_system_and_server (server_dict [key])
                except KeyError as ke:
                    self.error ('Problem with element ' + key)
                sys.stdout.write ('.')

        elif inputtype == 'SYSINFO':

            try:
                b_commit = True
                f = open (filename, "r")
                lines = f.readlines ()

                # remove header line
                lines = lines[1:] 
                sqlh = SQLHelper (self)
                for line in lines:
                    items_arr = line.split (",")

                    if len(items_arr) != 12 or line.startswith ('[!]'):
                        sys.stderr.write ('!')
                        continue

                    host_sysnr = items_arr[0]
                    dbtype = items_arr[1]
                    sysid = items_arr [2]
                    ip_address = items_arr [3] 
                    kernelversion = items_arr [4]
                    # instance_name nr 5
                    # host name nr 6
                    abapversion = items_arr [7]
                    # OS 8
                    # nochmal sysid 9
                    # nochmal hostname 10

                    hs_arr = host_sysnr.split (':')
                    host = hs_arr [0]
                    sysnr = hs_arr [1]  

                    #
                    # Jetzt machen wir folgendes: Erstmal den PK des Eintrags
                    # aus der Tabelle HOSTS anhand von param 0 ermitteln. Der
                    # kann inkorrekt sein!
                    # Da wir aber vermutlich schon mit einer nicht geratenen
                    # Liste loslaufen sollte das passen
                    #
                    # Dann: Die Instanz anlegen. Die sollte auch da sein!
                    # Dann: SYSTEMS mit DB und Versionen aktualisieren.
                    #
                    try:
                        sqlh.openconnection ()
                        sqlh.update_with_sysinfo_infos (host, ip_address, sysnr, sysid, kernelversion, abapversion, dbtype)
                    except:
                        self.error ('Exception happened with system {}, host {}, sysnr{}'.format (sysid, host, sysnr))
                        b_commit = False
                    finally:
                        sqlh.closeconnection (b_commit)

            finally:
                f.close ()

        elif inputtype == 'SAPWNGUIXS':
            f = open (filename, "r")
            lines = f.readlines ()
            #pdb.set_trace()
            for line in lines:
                try:
                    appserverinfo = json.loads (line)
                except json.decoder.JSONDecodeError as e:
                    self.error ("Line " + line + " cannot be json-parsed.")
                    continue

                keyobj = appserverinfo [0]
                #
                # Wenn da ein Fehler ist, so steht er im ersten Array-Element
                # des Arrays.
                # So sieht die Zeile aus:
                #
                # [{ "key" : "isdascs.wdf.sap.corp:20014" },["Error: Thu Dec  5 12:52:41 2024","Description: partner 'isdascs.wdf.sap.corp:20014' not reached"," ","Release: 753","Component: NI (network interface), version 40","rc = -10","Module: D:/depot/bas/SAPGUIForJava_780_REL/bas_753_REL/src/base/ni/nixxi.cpp","Line: 3454","Detail NiPConnect2: 10.67.36.24:20014","System Call: connect","Error No: 10061","'WSAECONNREFUSED: Connection refused'"]]
                #
                poterrorobj = appserverinfo[1][0] 

                if poterrorobj[:7] == "Error: ":  
                    #
                    # Und in dem zweiten Array-Element des
                    # Arrays steht dann eine vernünftige Fehlermeldung.
                    #
                    self.error (appserverinfo[1][1] )
                    continue

                msg_server_host_port = keyobj ['key']

                #
                # First get SYSTEM rowid for this msg server
                #
                harr = msg_server_host_port.split (":")
                host = harr [0]
                port = int(harr [1])
                result = self.query ("select system from INSTANCES where host in (select rowid from hosts " + 
                                     "where (ip_address = ? or host = ?)) and port=? and (type=? or type=?)", [
                                     host, host, port, INSTANCE_TYPE_MSGSERVER, INSTANCE_TYPE_BOTH])
                
                if len(result) == 0:
                    self.error ("Cannot find entry in instances for " + msg_server_host_port)
                    continue

                system_rowid = result [0][0]
                b_commit = True
                sqlh = SQLHelper (self)
                sqlh.openconnection ()

                # For each entry we have
                # an appserver
                for appserver in appserverinfo [1]:
                    p = re.compile ("([^ ]*)[ ]*([^ ]*)[ ]*([^ ]*)[ ]*([^ ]*).*")
                    m = p.match (appserver)
                    if m is None:
                        self.error ("Entry " + appserver + " cannot be parsed")
                        continue

                    as_type = m.group (1)
                    instance_name = m.group (2) # has the form 
                    host = m.group (3)
                    port = int(m.group (4))

                    #
                    # 
                    #
                    p = re.compile ("[1-9][0-9]{0,2}\.[1-9][0-9]{0,2}\.[1-9][0-9]{0,2}\.[1-9][0-9]{0,2}")
                    m = p.match (host)
                    is_ip = m is not None

                    if is_ip:
                        host_rowid = sqlh.insert_or_get_rowid_hosts (host)
                    else:
                        host_rowid = sqlh.insert_or_get_rowid_hosts (None, host)

                    # def insert_or_update_instance (self, row_id_system: int, row_id_host: int, type: str, port: int, sysnr: int):
                    sqlh.insert_or_update_instance (system_rowid, host_rowid, INSTANCE_TYPE_DIALOG, port, port % 100)

                sqlh.closeconnection (b_commit)

            
