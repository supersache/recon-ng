# module required for framework integration
from recon.core.module import BaseModule
# mixins for desired functionality
from recon.mixins.resolver import ResolverMixin
from recon.mixins.threads import ThreadingMixin 
# module specific imports
import os
import re
from lxml import etree

class Module(BaseModule, ResolverMixin, ThreadingMixin):

    # modules are defined and configured by the "meta" class variable
    # "meta" is a dictionary that contains information about the module, ranging from basic information, to input that affects how the module functions
    # below is an example "meta" declaration that contains all of the possible definitions

    meta = {
        'name': 'SAP Schema Creation',
        'author': 'Kai Ullrich',
        'version': '0.1',
        'description': 'Creates the initial database table for the SAP use case',
        
    }


    # mandatory method
    # the second parameter is required to capture the result of the "SOURCE" option, which means that it is only required if "query" is defined within "meta"
    # the third parameter is required if a value is returned from the "module_pre" method
    def module_run(self):
        self.query ('CREATE TABLE SYSTEMS (sysid TEXT, krnlv TEXT, abapv TEXT, database TEXT, created TEXT, updated TEXT, description TEXT, PRIMARY KEY (sysid));')
        self.output ('SYSTEMS created')
        # HIER FEHLT WELCHER TYP DIE INSTANZ IST!!!!!
        self.query ("""CREATE TABLE INSTANCES (sysnr INTEGER, host INTEGER, 
                                            system INTEGER, type TEXT, port INTEGER,
                                            created TEXT, updated TEXT, 
                                            FOREIGN KEY(host) REFERENCES HOSTS(rowid),
                                            FOREIGN KEY(system) REFERENCES SYSTEMS(rowid),
                                            PRIMARY KEY (sysnr, host, type));""")
        self.output ('INSTANCES created')

