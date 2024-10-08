import os,sys
import rpyc
import json
f = open('../config.json')
config = json.load(f)
class DataNodeServerService(rpyc.Service):
  def exposed_put(self, blockName, data):
    folder = os.path.join(config['rootFolder'],'localhost_'+str(sys.argv[1:][0]))
    with open(os.path.join(folder,blockName), 'wb') as f:
      f.write(data)
    return ('Putting block "{}" in localhost:{}'.format(blockName,str(sys.argv[1:][0])))

  def exposed_get(self, blockName):
    folder = os.path.join(config['rootFolder'],'localhost_'+str(sys.argv[1:][0]))
    block_path = os.path.join(folder,blockName)
    if not os.path.isfile(block_path): 
      return None
    with open(block_path, 'rb') as file:
      data = file.read()
    return data
  def exposed_delBlock(self,blockName):
     folder = os.path.join(config['rootFolder'],'localhost_'+str(sys.argv[1:][0]))
     if os.path.exists(os.path.join(folder,blockName)):
        os.remove(os.path.join(folder,blockName))
  def exposed_truncate(self):
    folder = os.path.join(config['rootFolder'],'localhost_'+str(sys.argv[1:][0]))
    files = os.listdir(folder)
    for f in files:
        try:
            os.remove(os.path.join(folder,f))
        except:
            continue
if __name__=="__main__":
  from rpyc.utils.server import ThreadedServer
  args = sys.argv[1:]
  t = ThreadedServer(DataNodeServerService(), port=int(args[0]))
  print('Server details ({}, {})'.format(t.host, int(args[0])))
  t.start()