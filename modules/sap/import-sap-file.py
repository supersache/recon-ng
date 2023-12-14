# module required for framework integration
from recon.core.module import BaseModule
# mixins for desired functionality
from recon.mixins.resolver import ResolverMixin
from recon.mixins.threads import ThreadingMixin
# module specific imports
import os
import re
from lxml import etree
import os
import sys
from kysecc.helpers.sqlite_helpers import SQLHelper

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
LANDSCAPE_XML (SAPUILandscapeCentral.xml)""")
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


