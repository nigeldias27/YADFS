import firebase_admin
from firebase_admin import credentials
from threading import Thread
import rpyc
import json
import time
import os
import glob
import random

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

def traverseDict(dictionary,path=config['logicalFolder']):
    files = []
    if 'blocks' in dictionary.keys():
            dictionary['path'] = path
            return [dictionary]
    else:
        for x in dictionary:
            if x!='type':
                files.extend(traverseDict(dictionary[x],os.path.join(path,x)))
        return files
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
        for name in range(len(config['dataNodes'])):
            dn = config['dataNodes'][name].split(":")
            if this_ss_is_alive(dn[0], dn[1]):
                if self.dnAlive[name] ==0:
                    conn = rpyc.connect(config['dataNodes'][name].split(':')[0],int(config['dataNodes'][name].split(':')[1]))
                    conn.root.truncate()
                    conn.close()
                self.dnAlive[name] = 1 # set alive flag if was not before
            else:
                if self.dnAlive[name] == 1:
                    ref = db.reference(config['logicalFolder'])
                    values = ref.get()
                    filesMetadata = traverseDict(values)
                    for fileMetadata in filesMetadata:
                        for block in fileMetadata:
                            if block != 'blocks' and block!="path" and config['dataNodes'][name] in fileMetadata[block]:
                                    availableDN = [config['dataNodes'][temp] for temp in range(len(config['dataNodes'])) if self.dnAlive[temp]==1]
                                    dnDataCanBeForwarded = list(set(availableDN) - set(fileMetadata[block]))
                
                                    dnDataExists = list((set(availableDN) & set(fileMetadata[block]))-{config['dataNodes'][name]})

                                    if len(dnDataCanBeForwarded)==0 or len(dnDataExists)==0:
                                        print("Sufficient Data nodes are not active to maintain replication factor")
                                    else:
                                        fromDN = random.choice(dnDataExists)
                              
                                        toDN = random.choice(dnDataCanBeForwarded)
                            
                                        print('Forwarding block',block,'from',fromDN,'to',toDN)
                                        connfrom = rpyc.connect(fromDN.split(':')[0],int(fromDN.split(':')[1]))
                                        data = connfrom.root.get(block)
                                        connfrom.close()
                                        connto = rpyc.connect(toDN.split(':')[0],int(toDN.split(':')[1]))
                                        connto.root.put(block,data)
                                        connto.close()
                                        blockDNcopy = list(fileMetadata[block])
                                        for t in range(len(blockDNcopy)):
                                            if blockDNcopy[t]==config['dataNodes'][name]:
                                                blockDNcopy[t]=toDN
                 
                                        db.reference(fileMetadata['path']).update({block:blockDNcopy})

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
        files = ref.get(False,False)
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
    def exposed_rmFile(self,path):
        ref = db.reference(path)
        blocks = ref.get()
        if blocks==None or 'blocks' not in blocks.keys():
            return ("Err: Not a file")
        else:
            for block in blocks:
                if block!='blocks':
                    for datanode in blocks[block]:
                        conn = rpyc.connect(datanode.split(':')[0],int(datanode.split(':')[1]))
                        conn.root.delBlock(block)
                        conn.close()
            db.reference(path).set({})
            return ("Removed File")

if __name__=="__main__":
  from rpyc.utils.server import ThreadedServer
  t = ThreadedServer(NameNodeServerService(), port=config['nameNode'][1])
  print('Server details ({}, {})'.format(t.host, config['nameNode'][1]))
  t.start()