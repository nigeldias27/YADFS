# Big Data project

Libraries to be installed

- firebase_admin
- rpyc

Starting the cli:
python cli.py

Starting the namenode:

```
cd namenode
python namenode.py
```

Starting the datanode:
python datanode.py [portno]

Port no for the 3 datanodes in the config file are 1235,1236,1237

```
cd datanode
python datanode.py 1235
```

Change the rootFolder,logicalFolder attribute in the config file
