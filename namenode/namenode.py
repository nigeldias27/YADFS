import firebase_admin
from firebase_admin import credentials
from threading import Thread
import rpyc
import json
import time
f = open('../config.json')
config = json.load(f)
cred = credentials.Certificate("elections-255d1-firebase-adminsdk-qckpp-6ad0a7522d.json")
default_app = firebase_admin.initialize_app(cred, {
	'databaseURL':"https://elections-255d1.firebaseio.com"
	})

from firebase_admin import db
import os
from pathlib import Path
import json

def createDNFolder():
   for x in config["dataNodes"]:
      print(os.path.join(config['rootFolder'],x.replace(':','_')))
      Path(os.path.join(config['rootFolder'],x.replace(':','_'))).mkdir(parents=True, exist_ok=True)

def this_ss_is_alive(ip, port):
  try:
    conn = rpyc.connect(ip, int(port))
    conn.close()
    return True
  except ConnectionRefusedError:
    return False
class NameNodeServerService(rpyc.Service):
    dnAlive = [1 for x in config['dataNodes']]
    def __init__(self):
        print('Name server started...')
        createDNFolder()
        thread = Thread(target=self.check_aliveness, args=(0,), daemon=True)
        thread.start()
    def check_aliveness(self, check_count):
    # check aliveness & take actions
        check_count += 1
        print('Checking aliveness of storage servers...({})'.format(check_count))
        modified = False
        for name in range(len(config['dataNodes'])):
            dn = config['dataNodes'][name].split(":")
            if this_ss_is_alive(dn[0], dn[1]):
                self.dnAlive[name] = 1 # set alive flag if was not before
            else:
                self.dnAlive[name] = 0
        
        # sleep for this time
        print(self.dnAlive)
        time.sleep(int(config['alivenessInterval']))
        # repeat the process again
        self.check_aliveness(check_count)
    def exposed_mkdir(self,path):
       parFileSplit = os.path.split(path)
       ref = db.reference(parFileSplit[0])
       x = ref.get(False,True)
       if x==None or 'blocks' in x.keys():
           return ("Err: Path does not exist")
       ref.update({parFileSplit[1]:{"type":"Dir"}}) 
       return "Directory Created"
    def exposed_vt(self,path):
       ref = db.reference(path)
       return json.dumps(ref.get(False,False))
    def exposed_ls(self,path):
        ref = db.reference(path)
        files = ref.get(False,True)
        print(files)
        return json.dumps(files)
    def exposed_cd(self,path):
       ref = db.reference(path)
       x = ref.get(False,True)
       if x==None or 'blocks' in x.keys():
           return ("Err: Path does not exist")
       return True
    def exposed_aliveServers(self):
       return json.dumps([config['dataNodes'][x] for x in range(len(config['dataNodes'])) if self.dnAlive[x]==1])
    def exposed_put(self,path,blockDict):
       parFileSplit = os.path.split(path)
       ref = db.reference(parFileSplit[0])
       ref.update({parFileSplit[1]:json.loads(blockDict)})
    def exposed_get(self,path):
       ref = db.reference(path)
       metadata = ref.get(False,False)
       if metadata==None or 'blocks' not in metadata.keys():
                return "Err: Not a file"
       return json.dumps(metadata)
    def exposed_rmdir(self,path):
        ref = db.reference(path)
        metadata = ref.get(False,True)
        metadataKeys = metadata.keys()
        if 'type' in metadataKeys and len(metadataKeys)==1:
            db.reference(path).set({})
            return "Directory Removed"
        else:
            return "Error because the path is either a file or a non-empty folder"
    def exposed_mv(self,src,dest):
        ref1 = db.reference(src)
        metadata = ref1.get()
        if 'blocks' in metadata:
            parFileSplit = os.path.split(dest)
            ref2 = db.reference(parFileSplit[0])
            ref2.update({parFileSplit[1]:metadata})
            db.reference(src).set({})
            return "Moved to "+dest
        else:
            return "Error with src path"
    def exposed_cp(self,src,dest):
        ref1 = db.reference(src)
        metadata = ref1.get()
        if 'blocks' in metadata:
            parFileSplit = os.path.split(dest)
            ref2 = db.reference(parFileSplit[0])
            ref2.update({parFileSplit[1]:metadata})
            return "Copied to "+dest
        else:
            return "Error with src path"
if __name__=="__main__":
  from rpyc.utils.server import ThreadedServer
  t = ThreadedServer(NameNodeServerService(), port=config['nameNode'][1])
  print('Server details ({}, {})'.format(t.host, config['nameNode'][1]))
  t.start()