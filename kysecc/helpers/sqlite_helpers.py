import sqlite3
import os
from datetime import datetime

INSTANCE_TYPE_DIALOG = 'dialog'
INSTANCE_TYPE_MSGSERVER = 'msgserver'
INSTANCE_TYPE_BOTH = 'msgserver+dialog'

class SQLHelper:

    conn = None

    def __init__(self, bc):
        self.me = bc

    def insert_or_get_rowid_systems (self, sysid: str, description: str, krnlv = '', abapv = '', database = ''):
        rowid = self.me.query ("select rowid from SYSTEMS where sysid = ?", [sysid])

        if len(rowid) > 0:
            return rowid [0][0]

        rowid = self.insert_with_return_rowid ("INSERT INTO SYSTEMS (SYSID,DESCRIPTION,CREATED,KRNLV,ABAPV) values (?,?,?,?,?)", [sysid, description, datetime.now().isoformat(),krnlv, abapv])
        
        return rowid
    

    def insert_or_update_systems (self, sysid: str, description: str, krnlv = '', abapv = '', database = ''):
        result = self.me.query ("select rowid from SYSTEMS where sysid = ?", [sysid])

        if len(result) > 0:
            rowid = result [0] [0] 
            self.conn.execute ("UPDATE SYSTEMS set KRNLV=?, ABAPV=?, database=? where rowid = ?", [krnlv, abapv, database, rowid] )
            return rowid
        
        rowid = self.insert_with_return_rowid ("INSERT INTO SYSTEMS (SYSID,DESCRIPTION,CREATED,KRNLV,ABAPV) values (?,?,?,?,?)", [sysid, description, datetime.now().isoformat(), krnlv, abapv])
        
        return rowid    


    def insert_or_get_rowid_hosts (self,  ip: str, hostname=None):

        if ip is not None:
            rowid = self.me.query ("select rowid from HOSTS where ip_address=?", [ip])
            if len(rowid) > 0:
                return rowid [0][0]
        else:
            rowid = self.me.query ("select rowid from HOSTS where host=?", [hostname])
            if len(rowid) > 0:
                return rowid [0][0]
        
        if ip is None:
            rowid = self.insert_with_return_rowid ("INSERT INTO HOSTS(host) VALUES (?)", [hostname])
        elif hostname is None:
            rowid = self.insert_with_return_rowid ("INSERT INTO HOSTS(ip_address) VALUES (?)", [ip])
        else:
            rowid = self.insert_with_return_rowid ("INSERT INTO HOSTS(host,ip_address) VALUES (?, ?)", [hostname, ip])

        
        return rowid
    
    #
    # Hier sind folgende Fälle zu beachten:
    # 
    # o Primary key on SYSNR, HOST, TYPE
    #
    def insert_or_update_instance (self, row_id_system: int, row_id_host: int, type: str, port: int, sysnr: int):
        #
        # first we need to check if exactly the same thing already exists
        #
        result = self.me.query ("select type from INSTANCES where host=? and sysnr=?", [row_id_host, sysnr])

        if len(result) > 0:
            if type == result[0][0]:
                # Genau diese Instanz ist schon da
                # Fehler oder müssen wir was machen?
                return
            else:
                # hier ist ein msgserver schon da und wir fügen dialog hinzu oder umgekehrt
                self.insert_with_return_rowid ("update INSTANCES set type=?,updated=? where host=? and sysnr=?", 
                                               [INSTANCE_TYPE_BOTH, datetime.now().isoformat(), row_id_host, sysnr])
        else:
            # Die Instanz existiert noch nicht, wir inserten
            self.insert_with_return_rowid ("INSERT INTO INSTANCES(sysnr,host,system,type,port,created) values (?,?,?,?,?,?)", 
                                           [sysnr, row_id_host, row_id_system, type, port, datetime.now().isoformat ()])

    #
    # Import die Infos aus dem Sysinfo Sweep
    #
    def update_with_sysinfo_infos (self, host: str, ip_address: str, sysnr: str, sysid: str, kernelversion: str, abapversion: str, dbtype: str):
        # Jetzt machen wir folgendes: Erstmal den PK des Eintrags
        # aus der Tabelle HOSTS anhand von param 0 ermitteln. Der
        # kann inkorrekt sein!
        # Da wir aber vermutlich schon mit einer nicht geratenen
        # Liste loslaufen sollte das passen
        #
        # Dann: Die Instanz anlegen. Die sollte auch da sein!
        # Dann: SYSTEMS mit DB und Versionen aktualisieren.
        #
        result = self.me.query ("select rowid from HOSTS where host=? or ip_address=?", [host, ip_address] )

        if len(result) == 0:
            # Den Host gibt's noch nicht. komisch, aber kann passieren.
            rowid_host = self.insert_with_return_rowid ("INSERT INTO HOSTS(host,ip_address) VALUES (?, ?)", [host, ip_address])
        elif len(result) == 1:
            rowid_host = result[0][0]
        else:
            # Mehr als ein Treffer ! Hier muss harmonisiert werden!!!
            self.me.warning ("{} {} already one item in HOSTS!".format (host, ip_address))
            # Dann updaten wir den ersten
            rowid_host = result[0][0]
            self.conn.execute ("update HOSTS set host=?, ip_address=? where rowid=?", 
                                [host, ip_address, rowid_host])
            
        #
        # Da wir bei der Verarbeitung der Instanz die SYSID brauchen, müssen wir uns
        # erst um die Tabelle SYSTEMS kümmern.
        #
        rowid_sysid = self.insert_or_update_systems (sysid, None, kernelversion, abapversion, dbtype)

        #
        # Jetzt checken ob die Instanz schon da ist. Hier nehmen wir wieder nur die erste
        # rowid von gerade, nehmen das Risiko einfach in Kauf, dass hier mehrfache Hosts
        # am Start sind.
        #
        i_sysnr = int (sysnr)
        result = self.me.query ("select rowid,type from INSTANCES where host=? and sysnr=? and (type=? or type=?)", [rowid_host, i_sysnr,INSTANCE_TYPE_DIALOG, INSTANCE_TYPE_BOTH] )

        #
        # es kann mit diesem Host und Sysnr nur einen DialogServer geben. Wenn es schon einen gibt, muss der 'dialog'
        # oder msgserver+dialog im type tragen
        #
        if len(result) > 1:
            self.me.error ("Check instances with host {} and sysnr{}".format (host, i_sysnr))
            raise DBException ("Hier stimmt was mit dem Primary KEy in Instances nicht.")
        elif len (result) == 0:
            #
            # Es gibt das Ding (erstaunlicherweise) noch nicht, dann legen wir es an.
            #
            self.insert_or_update_instance (rowid_sysid, rowid_host, INSTANCE_TYPE_DIALOG, 3300 + i_sysnr, i_sysnr)
        else:
            # len == 1, letzte Möglichkeit
            rowid_instance = result [0][0]
            instance_type = result [0][1]

            # wir müssen nur etwas tun wenn INSTANCE_TYPE_MSGSERVER im type steht
            if instance_type == INSTANCE_TYPE_MSGSERVER:
                self.conn.execute ("UPDATE INSTANCES set type=?,updated=? where rowid = ?", [INSTANCE_TYPE_BOTH, datetime.now().isoformat(), rowid_instance])

    #
    # open connection, keeps an internal connection object
    # and needs to be closed with commit or rollback
    #
    def openconnection (self):
        if self.conn is not None:
            #
            # Connection is already there, open not allowed
            #
            raise DBException ("Connection has not been opened")
        
        dbpath = os.path.join(self.me.workspace, 'data.db')
        self.conn = sqlite3.connect (dbpath)
        

    def insert_with_return_rowid (self, query, args=None):
        if self.conn is None:
            #
            # Connection is already there, open not allowed
            #
            raise DBException ("Connection has not been opened")
        
        if args is None:
            self.conn.execute (query)
        else:
            self.conn.execute (query, args)

        rowid_arr = self.conn.execute ("select last_insert_rowid()")

        return rowid_arr.lastrowid
        
    def closeconnection (self, b_commit: bool):
        if self.conn is None:
            #
            # Connection is already there, open not allowed
            #
            raise DBException ("Connection has not been opened")

        if b_commit:
            self.conn.commit ()
        else:
            self.conn.rollback ()

        self.conn = None


class DBException(Exception):
    
    def __init__(self, message):
        Exception.__init__(self, message)
