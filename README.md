# Big Data project

# YADFS - Yet Another Distributed File System

It consists of a Name Node (Server), Data Nodes, and a Command Line Interface (CLI) for interacting with the file system.

## Table of Contents

- Features
- Installation
- Usage

  - CLI
  - Name Node
  - Data Node

- Configuration

### Features

- <b>Distributed Storage</b>: Distribute file data across multiple Data Nodes for fault tolerance.
- <b>Metadata Management</b>:The Name Node handles metadata, ensuring proper organization and tracking of files.

### Installation

1. Clone the repository:

```bash
git clone https://github.com/Cloud-Computing-Big-Data/RR-Team-67-Yet-Another-Distributed-File-System-YADFS-.git
cd RR-Team-67-Yet-Another-Distributed-File-System-YADFS-
```

2. Installation

```bash
pip install rpyc
pip install firebase-admin
```

3. Configure `config.json`.

```json
{
  "blockSize": 500,
  "replicationFactor": 3,
  "rootFolder": "/var/YADFS",
  "logicalFolder": "/YADFS6",
  "alivenessInterval": 5,
  "nameNode": ["localhost", 1234],
  "dataNodes": ["localhost:1235", "localhost:1236", "localhost:1237"]
}
```

Port number for the 3 datanodes in the config file are 1235,1236,1237

4.Start the `NameNode`.

```
cd namenode
python3 namenode.py
```

NameNode Default Port No is `1234`.

5. Start `DataNode`

```
cd ..
cd datanode
python3 datanode.py <port>
```

6. Start CLI

```
cd ..
python3 cli.py
```

Commands in `COMMAND LINE INTERFACE`:

- ls : List all files and subdirectories in the present working directory
- mkdir [dir] : Create a directory
- cp [src] [dest] : Copy file
- mv [src] [dest] : Move file
- vt : Shows the virtual tree
- rmdir [dir] : Deletes the directory
- rm [file] : Deletes the file
- cd : Change current directory
- pwd : Present working directory
- get [localPath] [YADFSPath] : Get file present in yadfs to local file system
- put [localPath] [YADFSPath] : Put file present local file system into yadfs

Change the rootFolder,logicalFolder attribute in the config file
