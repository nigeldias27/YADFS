
import rpyc
import math
import os
from uuid import uuid4
from cmd import Cmd
import random
import json

ns_con = None
f = open('config.json')
config = json.load(f)


def connect_ns():
    global ns_con
    print('Connecting to Name Node')
    ns_con = rpyc.connect(config["nameNode"][0],config["nameNode"][1])
    if ns_con == None:
        print("Could not connect to Name Node")
        return False
    else:
        print("Connected to Name Node!")
        return True
def traverseDict(dictionary,counter=0):
        if 'blocks' in dictionary.keys():
            return
        for x in dictionary:
            if x!="type":
                print("     "*counter,x,sep="|-")
                traverseDict(dictionary[x],counter+1)
class CLI(Cmd):
    global ns_con
    ROOT_PATH = config['logicalFolder']
    CURRENT_PATH = ROOT_PATH
    intro = "Welcome to YADFS.\n"
    prompt = "YADFS >>"
    def do_mkdir(self,dir):
        callback = ns_con.root.mkdir(os.path.join(self.CURRENT_PATH,dir))
        print(callback)
    def do_rmdir(self,dir):
        print(dir)
    def do_vt(self,_):
        callback = ns_con.root.vt(self.ROOT_PATH)
        print("YADFS")
        traverseDict(json.loads(callback),1)
    def do_cd(self,dir):
        if dir=='..':
           self.CURRENT_PATH = os.path.dirname(self.CURRENT_PATH)
           print("Changed to: ",self.CURRENT_PATH)
        else:
            checkFileExists = ns_con.root.cd(os.path.join(self.CURRENT_PATH,dir))
            if checkFileExists=="Err: Path does not exist":
                print(checkFileExists)
            else:
                self.CURRENT_PATH = os.path.join(self.CURRENT_PATH,dir)
                print("Changed to: ",self.CURRENT_PATH)
    def do_pwd(self,args):
        print(self.CURRENT_PATH)
    def do_ls(self,dir):
        lsFiles = ns_con.root.ls(self.CURRENT_PATH)
        for x in json.loads(lsFiles):
            if x!="type":
                print(x)

    def do_get(self,arg):
        localPath = arg.split(' ')[0]
        yadfsPath = arg.split(' ')[1]   
        metadataString = ns_con.root.get(os.path.join(self.CURRENT_PATH,yadfsPath))
        if metadataString=="Err: Not a file":
            print("Err: Not a file")
        else:
            metadata = json.loads(metadataString)
            with open(localPath, "wb") as lf:
                for blockName in metadata['blocks']:

                    for ipPort in metadata[blockName]:
                        try:
                            conn = rpyc.connect(ipPort.split(':')[0],int(ipPort.split(':')[1]))
                            data = conn.root.get(blockName)
                            if data == None:
                                continue
                            else:
                                lf.write(data)
                            conn.close()
                            break
                        except:
                            continue
                    
    def do_put(self,arg):
        localPath = arg.split(' ')[0]
        yadfsPath = arg.split(' ')[1]
        if not os.path.exists(localPath):
            print('Local file does not exist')
        else:
            localSize = os.path.getsize(localPath)
            blocks = math.ceil(localSize/config['blockSize'])
            blockDict = {}
            blockDict['blocks'] = []
            dataNodes = json.loads(ns_con.root.aliveServers())
            if(config['replicationFactor']>len(dataNodes)):
                print('Insufficient Datanodes')
            print('File has been split into',blocks,'blocks')
            with open(localPath, 'rb') as lf:
                blockData = lf.read(config['blockSize'])
                while blockData:
                    blockName = str(uuid4())
                    specificDataNodes = random.sample(dataNodes,config['replicationFactor'])
                    for ipPort in specificDataNodes:
                        conn = rpyc.connect(ipPort.split(':')[0],int(ipPort.split(':')[1]))
                        conn.root.put(blockName,blockData)
                        conn.close()
                    blockDict[blockName] = specificDataNodes
                    blockDict['blocks'].append(blockName)
                    blockData = lf.read(config['blockSize'])
            ns_con.root.put(os.path.join(self.CURRENT_PATH,yadfsPath),json.dumps(blockDict))

if connect_ns():
    CLI().cmdloop()