import os
import pyarrow as pa
import glob
import json

with open('directories.json') as f:
    data = json.load(f)

os.environ['HADOOP_HOME'] = "/home/danielbeach/hadoop/etc/hadoop"
os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-8-openjdk-amd64/jre'
os.environ['ARROW_LIBHDFS_DIR'] =  '/home/danielbeach/hadoop/lib/native'
os.environ['HADOOP_CONF_DIR'] = '/home/danielbeach/hadoop/etc/hadoop'

hadoop_jars  = ''
for filename in glob.glob('/home/danielbeach/hadoop/share/hadoop/**/*.jar', recursive=True):
    hadoop_jars += filename + ':'

os.environ['CLASSPATH'] = hadoop_jars

hdfs = pa.hdfs.connect(host='default', port=9000, driver='libhdfs', user='danielbeach')

for k,v in data.items():
    for item in v:
        directory = "{}/{}".format(k, item)
        if hdfs.exists(path=directory) == False:
            hdfs.mkdir(path=directory)
        else:
            print(hdfs.ls(path=directory))
